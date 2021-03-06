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


PendingTask  = namedtuple("PendingTask" , ["id", "event", "args", "status", "response"])
FinishedTask = namedtuple("FinishedTask" , ["id", "event", "args", "status", "response"])


class Kerground:
    
    def __init__(self, workers_path=None, storage_path=None):

        if not workers_path:
            workers_path = os.getenv("KERGROUND_WORKERS_PATH")
        if not storage_path:
            storage_path = os.getenv("KERGROUND_STORAGE_PATH")
            
        if not workers_path or storage_path:
            workers_path = os.getcwd()
            storage_path = os.getcwd()


        # TODO add timeout's, cron_jobs
        self.ker_dir = Kerground.prep_storage_dir(storage_path)
        self.events, self.events_collected = Kerground.collect_events(workers_path)
        self.create_db()


    @staticmethod
    def prep_storage_dir(storage_path):

        if ".Kerground" not in storage_path:
            storage_path = os.path.join(storage_path, ".Kerground")

        if not os.path.exists(storage_path):
            os.mkdir(storage_path)

        return storage_path


    @staticmethod
    def collect_events(workers_path):

        events = {}
        events_collected = []
        for root, dirs, files in os.walk(workers_path): 
            for file in files:
                if file.endswith("_worker.py"):

                    file_path = os.path.join(root, file)
                    spec, module, func_names = Kerground.get_module_data(file_path)
                    file_id = str(uuid.uuid5(uuid.NAMESPACE_OID, file_path))

                    events.update({
                        file_id: {
                            "file_path": file_path,
                            "spec": spec,
                            "module": module,
                            "events": func_names
                        }
                    })

                    events_collected.append(func_names)

        events_collected = list(itertools.chain.from_iterable(events_collected))
        duplicate_events = [x for n, x in enumerate(events_collected) if x in events_collected[:n]]
        if duplicate_events:
            raise Exception(f"Function names from worker files must be unique!\nCheck function(s): {','.join(duplicate_events)}")
        
        # print(events_gathered)

        return events, events_collected


    def execute_sql(self, sql):

        self.sqlpath = os.path.join(self.ker_dir, "tasks.db")
        
        with sqlite3.connect(self.sqlpath) as conn:
            if isinstance(sql, tuple):
                if sql[0].upper().startswith("SELECT"):
                    res = conn.execute(sql[0], sql[1]).fetchall()
                    res = [r[0] for r in res]
                else:
                    res = conn.execute(sql[0], sql[1])
            elif isinstance(sql, str): 
                res = conn.execute(sql)
            else:
                raise ValueError("SQL statement must be either a string or a tuple(sql, and params)!")
        return res
        

    def create_db(self):
        sql_statement = "CREATE TABLE IF NOT EXISTS tasks (id TEXT NOT NULL, status TEXT NOT NULL);"
        self.execute_sql(sql_statement)

    def tasks_by_status(self, status: str):
        sql_statement = "SELECT id FROM tasks WHERE status = ?;", (status,)
        res = self.execute_sql(sql_statement)
        return res


    def pending(self) : return self.tasks_by_status("pending")
    def running(self) : return self.tasks_by_status("running")
    def finished(self): return self.tasks_by_status("finished")
    def failed(self)  : return self.tasks_by_status("failed")
        

    @staticmethod
    def get_module_data(file_path):

        module_name = os.path.basename(file_path).split(".py")[0]
        spec = spec_from_file_location(module_name, file_path)
        module = module_from_spec(spec)
        spec.loader.exec_module(module)
        func_names = [f[0] for f in getmembers(module, isfunction)]

        return spec, module, func_names

    def save(self, task):
        save_path = os.path.join(self.ker_dir, f"{task.id}.pickle")
        with open(save_path, "wb") as pkl:
            pickle.dump(task, pkl)  
        # logging.warning(f"New task added:\n{save_path}")

    def load(self, id):
        save_path = os.path.join(self.ker_dir, f"{id}.pickle")
        with open(save_path, "rb") as pkl:
            task = pickle.load(pkl)
        # logging.warning(f"Task loaded:\n{save_path}")
        return task
    
    def get_response(self, id):
        task = self.load(id)
        return task.response

    def send(self, event, *args):
        
        if isfunction(event):
            event = event.__name__

        if not isinstance(event, str): 
            raise Exception("Event must be a string or function!")

        task = PendingTask(str(uuid.uuid4()), event, args, "pending", None)
        self.save(task)

        sql_statement = "INSERT INTO tasks (id, status) VALUES (?, ?);", (task.id, task.status,)
        self.execute_sql(sql_statement)
        
        return task.id

    def status(self, id):
        sql_statement = "SELECT status FROM tasks WHERE id = ?;", (id,)
        res = self.execute_sql(sql_statement)
        return res[0] if res else "id not found"


    def execute(self, id):
        task = self.load(id)
        sql_statement = "UPDATE tasks SET status = ? WHERE id = ?;", ("running", id,)
        self.execute_sql(sql_statement)

        try:

            ftask = None
            for _, data in self.events.items():
                if task.event in data["events"]:
                    
                    # import module
                    data["spec"].loader.exec_module(data["module"]) 
                
                    # execute func
                    if task.args: 
                        response = getattr(data["module"], task.event)(task.args) 
                    else: 
                        response = getattr(data["module"], task.event)()

                    ftask = FinishedTask(id, task.event, task.args, "finished", response)
                    break

            if not ftask: raise Exception(f'Event "{task.event}" with id "{id}" not found!')
            self.save(ftask)

            sql_statement = "UPDATE tasks SET status = ? WHERE id = ?;", (ftask.status, ftask.id,)
            self.execute_sql(sql_statement)

            return response

        except:

            ftask = FinishedTask(id, task.event, task.args, "failed", traceback.format_exc())
            
            sql_statement = "UPDATE tasks SET status = ? WHERE id = ?;", (ftask.status, ftask.id,)
            self.execute_sql(sql_statement)

            self.save(ftask)


    def run(self):

        while True:

            pending_tasks = self.pending()  
            if not pending_tasks: 
                time.sleep(1)
                continue

            logging.warning("pending_tasks: " + str(pending_tasks))

            with ProcessPoolExecutor() as executor:
                # Sync
                [executor.submit(self.execute(task)) for task in pending_tasks]

            logging.warning("Waiting...")
    


# Initializer for api's
ker = Kerground()


def cli():

    parser = argparse.ArgumentParser(
        prog="kerground", description="Run kerground background worker."
    ) 

    parser.add_argument(
        "--workers-path", type=str, default=".", 
        help="Path to *_worker.py files from which events will be collected."
    )
    parser.add_argument(
        "--storage-path", type=str, default=".", 
        help="Path needed to store kerground files."
    )
    
    args = parser.parse_args()
    
    workers_path = os.path.abspath(args.workers_path)
    storage_path = os.path.abspath(args.storage_path)

    # logging.warning("KERGROUND_WORKERS_PATH " + workers_path)
    # logging.warning("KERGROUND_STORAGE_PATH " + storage_path)

    # # 
    # os.environ["KERGROUND_WORKERS_PATH"] = workers_path
    # os.environ["KERGROUND_STORAGE_PATH"] = storage_path

    logging.warning("\n\nKERGROUND READY\n\n")

    Kerground(
        workers_path, storage_path
    ).run()



    

if __name__ == "__main__": 
    cli()
