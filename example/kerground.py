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

from dotenv import load_dotenv
load_dotenv()


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

PendingTask  = namedtuple("PendingTask" , ["id", "event", "args", "status", "response"])
FinishedTask = namedtuple("FinishedTask" , ["id", "event", "args", "status", "response"])


class Kerground:
    """
        How it works:

        - Create a file like `my_worker.py` (must end in `_worker.py`)
        - Start adding functions (these will be our events)
        
        ```py
        # my_worker.py

        def long_task(params):
            # some work 
            return

        def another_long_task():
            # some work 
            return

        ```
        - Function names listed in worker files must be unique
        - Next, create another file let's say `my_api.py` (a file that sends 'events')
        
        ```py
        # my_api.py
        import Webframeork
        from kerground import Kerground

        # You instantiate `Kerground` class in one place and import `wk` in your bluprints/route files.
        wk = Kerground() 

        @app.post("/long-task")
        def long_task():
            id = wk.send('long_task', *requests.params)
            # you will receive an id which you can use howerver you want
            # here we send it to frontend to ask later if task is done
            return {'status': 'request received please wait', 'id': id}, 200

        @app.post("/long-task/<id>")
        def long_task():
            status = wk.status(id)
            return {'status': 'success', 'results': results}, 200

        ```
                
        By default data needed by kerground will be stored in the package base path.
        Optionally you can set `KERGROUND_STORAGE` environment variable to another path 
        and that path will be used instead.

        

    """

    def __init__(self):
        
        # TODO add timeout"s, cron_jobs
        self.worker_dir = Kerground.prep_worker_dir()
        self.events = self.gather_events()
        self.sqlpath = os.path.join(self.worker_dir, "tasks.db")
        self.create_db()


    def execute_sql(self, sql):
        
        try:
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
        except:
            logging.warning("[ERROR] " + str(sql))
            logging.warning(traceback.format_exc())
            

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
    def prep_worker_dir(persist_data):

        env_worker_dir = os.environ.get("KERGROUND_STORAGE")

        if not env_worker_dir: 
            raise Exception("KERGROUND_STORAGE not set in '.env'")

        if not os.path.exists(env_worker_dir):
            os.mkdir(env_worker_dir)
    
        return env_worker_dir


        # if env_worker_dir:
        #     worker_dir = env_worker_dir
        #     logging.warning(f"KERGROUND_STORAGE path found in environment variables.")
        # else:
        #     logging.warning(f"KERGROUND_STORAGE path not found, using by default\n{BASE_DIR}")
        #     worker_dir = os.path.join(BASE_DIR, ".Kerground")
        #     os.environ["KERGROUND_STORAGE"] = worker_dir
        
        # if not os.path.exists(worker_dir):
        #     logging.warning(f"Creating KERGROUND_STORAGE path:\n{worker_dir}")
        #     os.mkdir(worker_dir)

        # if not persist_data:
        #     logging.warning(f"Removing previous data from path:\n{worker_dir}")
        #     shutil.rmtree(worker_dir)
        #     os.mkdir(worker_dir)
        
        # logging.warning(f"\n\nKERGROUND READY!\n")
        # return worker_dir

        
    @staticmethod
    def get_module_data(file_path):

        module_name = os.path.basename(file_path).split(".py")[0]
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

                    events_gathered.append(func_names)

        events_gathered = list(itertools.chain.from_iterable(events_gathered))
        duplicate_events = [x for n, x in enumerate(events_gathered) if x in events_gathered[:n]]
        if duplicate_events:
            raise Exception(f"Function names from worker files must be unique!\nCheck function(s): {','.join(duplicate_events)}")
        
        # print(events_gathered)

        return events


    def save(self, task):
        save_path = os.path.join(self.worker_dir, f"{task.id}.pickle")
        with open(save_path, "wb") as pkl:
            pickle.dump(task, pkl)  
        # logging.warning(f"New task added:\n{save_path}")

    def load(self, id):
        save_path = os.path.join(self.worker_dir, f"{id}.pickle")
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
    




ker = Kerground()


if __name__ == "__main__":   

    if "run" in sys.argv:
        logging.warning("\n\nKERGROUND READY\n\n")
        ker.run()
    else:
        raise Exception("Type `python3 kerground.py run`")

    