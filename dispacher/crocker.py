import time
import sqlite3
import os, sys, shutil
import logging
import uuid, pickle
import itertools, traceback
from collections import namedtuple
from importlib.util import spec_from_file_location, module_from_spec
from inspect import getmembers, isfunction
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import argparse


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

PendingTask  = namedtuple('PendingTask' , ['id', 'event', 'args', 'status', 'response'])
FinishedTask = namedtuple('FinishedTask' , ['id', 'event', 'args', 'status', 'response'])


class Crocker:
    """
        How it works:
        - Initialize this class
        `from crocker import Crocker`
        `c = Crocker()`

        Events are string function names
        `c.send('func_name')` will append func to background processing

    """

    def __init__(self, worker_dir=None, persist_data=True):
        
        # TODO add timeout's, cron_jobs
        self.worker_dir = Crocker.prep_worker_dir(worker_dir, persist_data)
        self.events = self.gather_events()
        self.sqlpath = os.path.join(self.worker_dir, 'tasks.db')
        self.create_db()


    def execute_sql(self, sql):
        
        select_statement = False
        if isinstance(sql, tuple):
            if sql[0].upper().startswith("SELECT"): 
                select_statement = True 
                 
        try:
            with sqlite3.connect(self.sqlpath) as conn:
                if select_statement:
                    res = conn.execute(sql).fetchall()
                    res = [r[0] for r in res]
                else:
                    res = conn.execute(sql)

            return res
        except:
            logging.warning('[ERROR] ' + str(sql))
            logging.warning(traceback.format_exc())
            

    def create_db(self):
        sql_statement = "CREATE TABLE IF NOT EXISTS tasks (id TEXT NOT NULL, status TEXT NOT NULL);"
        self.execute_sql(sql_statement)

    def tasks_by_status(self, status):
        sql_statement = f"SELECT id FROM tasks WHERE status = {status}"
        res = self.execute_sql(sql_statement)
        return res


    # @property - works with init values...
    def pending(self): 
        return self.tasks_by_status('pending')
    
    def running(self): 
        return self.tasks_by_status('running')

    def finished(self): 
        return self.tasks_by_status('finished')

    def failed(self): 
        return self.tasks_by_status('failed')
        

    @staticmethod
    def prep_worker_dir(worker_dir, persist_data):

        env_worker_dir = os.environ.get("BACKGROUND_CROCKER_STORAGE")

        if env_worker_dir:
            worker_dir = env_worker_dir
        else:
            # logging.warning(env_worker_dir)
            worker_dir = os.path.join(BASE_DIR, '.Crocker')
            os.environ["BACKGROUND_CROCKER_STORAGE"] = worker_dir
        
        if not persist_data:
            shutil.rmtree(worker_dir)
            os.mkdir(worker_dir)
        
        if not os.path.exists(worker_dir):
            os.mkdir(worker_dir)

        # logging.warning(worker_dir)

        return worker_dir

        
    @staticmethod
    def get_module_data(file_path):

        module_name = os.path.basename(file_path).split('.py')[0]
        spec = spec_from_file_location(module_name, file_path)
        module = module_from_spec(spec)
        spec.loader.exec_module(module)
        func_names = [f[0] for f in getmembers(module, isfunction)]

        return spec, module, func_names


    def gather_events(self):
        
        events = {}
        events_gathered = []
        for root, dirs, files in os.walk(BASE_DIR): 
            for file in files:
                if (
                    file.endswith("_worker.py")
                    or
                    file.startswith("worker_") and file.endswith(".py")
                ):
                    
                    file_path = os.path.join(root, file)
                    spec, module, func_names = Crocker.get_module_data(file_path)
                    file_id = str(uuid.uuid5(uuid.NAMESPACE_OID, file_path))

                    events.update({
                        file_id: {
                            'file_path': file_path,
                            'spec': spec,
                            'module': module,
                            'events': func_names
                        }
                    })

                    events_gathered.append(func_names)

        events_gathered = list(itertools.chain.from_iterable(events_gathered))
        duplicate_events = [x for n, x in enumerate(events_gathered) if x in events_gathered[:n]]
        if duplicate_events:
            raise Exception(f"Function names from worker files must be unique!\nCheck function(s): {', '.join(duplicate_events)}")
        
        # print(events_gathered)

        return events


    def save_task(self, task):
        with open(os.path.join(self.worker_dir, f'{task.id}.pickle'), 'wb') as pkl:
            pickle.dump(task, pkl)  

    def load_task(self, id):
        with open(os.path.join(self.worker_dir, f'{id}.pickle'), 'rb') as pkl:
            task = pickle.load(pkl)
        return task
    
    def send(self, event, *args):
        
        if isfunction(event):
            event = event.__name__

        if not isinstance(event, str): 
            raise Exception("Event must be a string or function!")

        task = PendingTask(str(uuid.uuid4()), event, args, 'pending', None)
        self.save_task(task)

        sql_statement = "INSERT INTO tasks (id, status) VALUES (?, ?);", (task.id, task.status,)
        self.execute_sql(sql_statement)
        
        return task.id

    def status(self, id):
        sql_statement = f"SELECT status FROM tasks WHERE id = {id}"
        res = self.execute_sql(sql_statement)
        return res[0][0]


    def execute(self, id):
        
        task = self.load_task(id)
        sql_statement = f"UPDATE tasks SET status = {'running'} WHERE id = {id};"
        self.execute_sql(sql_statement)

        try:

            ftask = None
            for _, data in self.events.items():
                if task.event in data['events']:
                    
                    # import module
                    data['spec'].loader.exec_module(data['module']) 
                
                    # execute func
                    if task.args: 
                        response = getattr(data['module'], task.event)(task.args) 
                    else: 
                        response = getattr(data['module'], task.event)()

                    ftask = FinishedTask(id, task.event, task.args, 'finished', response)
                    break

            if not ftask: raise Exception(f'Event "{task.event}" with id "{id}" not found!')
            self.save_task(ftask)

            sql_statement = f"UPDATE tasks SET status = {ftask.status} WHERE id = {ftask.id};"
            self.execute_sql(sql_statement)

            return response

        except:

            ftask = FinishedTask(id, task.event, task.args, 'failed', traceback.format_exc())
            
            sql_statement = f"UPDATE tasks SET status = {ftask.status} WHERE id = {ftask.id};"
            self.execute_sql(sql_statement)

            self.save_task(ftask)




class CrockerRunner(Crocker): 

    def __init__(self):
        super().__init__()


    @classmethod
    def run(cls):
        w = cls()
        while True:
            pending_tasks = w.pending()  
            logging.warning('pending_tasks: ' + str(pending_tasks))

            if pending_tasks:
                with ProcessPoolExecutor() as executor:
                    for task, response in zip(pending_tasks, executor.map(cls().execute, pending_tasks)):
                        logging.warning(f'[EXECUTED] {task}:{response}')
            else:
                time.sleep(1)
                        


        

if __name__ == '__main__':   

    # parser = argparse.ArgumentParser(usage="\n\n --persist-data --Crocker-path /path/to/background-task/Crocker")
    # parser.add_argument('--persist-data', type=bool, nargs='False', default=False, help="If you want to keep background tasks after shutdown")
    # parser.add_argument('--Crocker-path', type=str, default=BASE_DIR, help="Path to where you want to keep background Crocker data, default is BASE_DIR")
    # args = parser.parse_args()
    # logging.warning(args)


    logging.warning("BACKGROUND_CROCKER_STORAGE: " + os.environ.get("BACKGROUND_CROCKER_STORAGE", "Not set"))

    CrockerRunner.run()
    
    