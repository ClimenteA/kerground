import time
import sqlite3
import os, shutil
import logging
import uuid, pickle
import traceback
from importlib.util import spec_from_file_location, module_from_spec
from inspect import getmembers, isfunction
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import argparse
import tempfile



# TODO
# add timeout's, cron_jobs
# Needs probably 3 files/classes 
# one for commun (sql etc), 
# one for event dispacher 
# one for cli
# maybe remove the need for a class?
# since all funcs from worker.py files are unique named auto generate event? like ker.event()?


def dump_pickle(data, file_path):
    with open(file_path, "wb") as pkl:
        pickle.dump(data, pkl)

def load_pickle(file_path):
    with open(file_path, "rb") as pkl:
        data = pickle.load(pkl)
    return data

def get_module_data(file_path):
    module_name = os.path.basename(file_path).split(".py")[0]
    spec = spec_from_file_location(module_name, file_path)
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    func_names = [f[0] for f in getmembers(module, isfunction)]
    return spec, module, func_names

def collect_events(workers_path):
    events = {}
    for root, dirs, files in os.walk(workers_path): 
        for file in files:
            if file.endswith("_worker.py"): 
                file_path = os.path.join(root, file)
                spec, module, func_names = get_module_data(file_path)
                # spec, module can't be pickeld 
                events.update({fn:file_path for fn in func_names})
    # logging.warning(events)
    return events


class Kerground:
    
    def __init__(self, _workers_path=None):

        self.storage_path = os.path.join(tempfile.gettempdir(), "kerground_storage")
        self.sqlpath = os.path.join(self.storage_path, "tasks.db")
        self._events_pickle = os.path.join(self.storage_path, 'events.pickle')

        if _workers_path: 

            if os.path.exists(self.storage_path):
                shutil.rmtree(self.storage_path)
                os.mkdir(self.storage_path)
            else:
                os.mkdir(self.storage_path)

            self.events = collect_events(_workers_path)
            dump_pickle(self.events, self._events_pickle)

        self.create_db()


    def execute_sql(self, sql):

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

    def stats(self):
        return {
            'pending': self.pending(),
            'running': self.running(),
            'finished': self.finished(),
            'failed': self.failed()
        }

    def save(self, task):
        dump_pickle(task, os.path.join(self.storage_path, f"{task['id']}.pickle"))
        
    def load(self, id):
        task = load_pickle(os.path.join(self.storage_path, f"{id}.pickle"))
        return task
    
    def get_response(self, id):
        task = self.load(id)
        return task['response']

    def send(self, event, *args):
        
        if isfunction(event):
            event = event.__name__

        if not isinstance(event, str): 
            raise Exception("Event must be a string or function!")

        task = {
            "id": str(uuid.uuid4()), 
            "event": event, 
            "args": args, 
            "status": "pending", 
            "response": None
        }
        
        self.save(task)

        sql_statement = "INSERT INTO tasks (id, status) VALUES (?, ?);", (task['id'], task['status'],)
        self.execute_sql(sql_statement)
        
        return task['id']

    def status(self, id):
        sql_statement = "SELECT status FROM tasks WHERE id = ?;", (id,)
        res = self.execute_sql(sql_statement)
        return res[0] if res else "id not found"


    def execute(self, id):
        task = self.load(id)
        sql_statement = "UPDATE tasks SET status = ? WHERE id = ?;", ("running", id,)
        self.execute_sql(sql_statement)

        try:

            func_path = self.events[task['event']]
            spec, module, _ = get_module_data(func_path)

            # import module
            spec.loader.exec_module(module)
            # execute func
            if task['args']: 
                response = getattr(module, task['event'])(*task['args']) 
            else: 
                response = getattr(module, task['event'])()

            task['status']   = "finished"
            task['response'] = response      

            self.save(task)

            sql_statement = "UPDATE tasks SET status = ? WHERE id = ?;", (task['status'], task['id'],)
            self.execute_sql(sql_statement)

            return response

        except:

            task['status']   = "failed"
            task['response'] = traceback.format_exc()  

            sql_statement = "UPDATE tasks SET status = ? WHERE id = ?;", (task['status'], task['id'],)
            self.execute_sql(sql_statement)

            self.save(task)


    def run(self):

        while True:

            pending_tasks = self.pending()  
            if not pending_tasks: 
                time.sleep(1)
                continue

            logging.warning(f"Working on {len(pending_tasks)} tasks...")

            # start_time = time.time()

            #Sync
            # [self.execute(task) for task in pending_tasks]

            #Multiprocessing
            with ProcessPoolExecutor() as executor:
                executor.map(self.execute, pending_tasks)

            # logging.warning(f"---- {time.time() - start_time}sec ----")

            logging.warning("Waiting...")



def cli():

    parser = argparse.ArgumentParser(
        prog="kerground", description="Run kerground background worker."
    ) 

    parser.add_argument(
        "--workers-path", type=str, default=".", 
        help="Path to *_worker.py files from which events will be collected."
    )
    
    args = parser.parse_args()

    workers_path = os.path.abspath(args.workers_path)
    logging.warning("\n\nKERGROUND READY\n\n")
    
    try:
        Kerground(workers_path).run()
    except KeyboardInterrupt:
        logging.warning("\n\nKERGROUND STOPPED\n\n")


if __name__ == "__main__": 
    cli()
