import logging, traceback
import sqlite3
import argparse
import tempfile
import time, os, shutil
import uuid, pickle

from threading import Thread
from concurrent.futures import ProcessPoolExecutor

from importlib.util import (
    spec_from_file_location, 
    module_from_spec
)
from inspect import (
    getmembers, 
    isfunction
)


logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] - %(message)s')



class ThreadWithResult(Thread):
    def __init__(self, group=None, target=None, name=None, args=(), kwargs={}, *, daemon=None):
        def function():
            self.result = target(*args, **kwargs)
        super().__init__(group=group, target=function, name=name, daemon=daemon)


def warn_timeout(func, sec, *args):

    t = ThreadWithResult(target=func, args=args)
    t.start()
    t.join(timeout=sec)
    if t.is_alive():
        logging.warning(
            f"""Function '{args[0]['event']}' takes more than {sec} {"second" if sec == 1 else "seconds"}!"""
        )
    t.join()
    return t.result


def execute_funcs(func, tasks):
    with ProcessPoolExecutor() as executor:
        executor.map(func, tasks)

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
    
    def __init__(self, workers_path=None):

        self.storage_path = os.path.join(tempfile.gettempdir(), "kerground_storage")
        self.sqlpath = os.path.join(self.storage_path, "tasks.db")

        if not os.path.exists(self.storage_path): 
            os.mkdir(self.storage_path)

        if workers_path: 
            if os.path.exists(self.storage_path):
                shutil.rmtree(self.storage_path)
                os.mkdir(self.storage_path)

            self.events = collect_events(workers_path)

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

    def send(self, event, /, *args, timeout=None):
        
        if isfunction(event):
            event = event.__name__

        if not isinstance(event, str): 
            raise Exception("Event must be a string or a function!")

        task = {
            "id": str(uuid.uuid4()), 
            "event": event, 
            "args": args, 
            "timeout": timeout,
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

    def set_status(self, id, status):
        sql_statement = "UPDATE tasks SET status = ? WHERE id = ?;", (status, id,)
        self.execute_sql(sql_statement)

    def update_task(self, task, status, response):
        self.set_status(task['id'], status)
        task['status']   = status
        task['response'] = response      
        self.save(task)

    def fetch_task(self, id):
        task = self.load(id)
        self.set_status(id, "running")
        func_path = self.events[task['event']]
        spec, module, _ = get_module_data(func_path)
        return task, spec, module

    def run_task(self, task, spec, module):
        # import module
        spec.loader.exec_module(module)
        # execute func
        if task['args']: 
            response = getattr(module, task['event'])(*task['args']) 
        else: 
            response = getattr(module, task['event'])()
        return response

    def execute(self, id):
        task, spec, module = self.fetch_task(id)
        try:
            response = warn_timeout(self.run_task, task['timeout'], task, spec, module)
            self.update_task(task, "finished", response)
            return response
        except:
            self.update_task(task, "failed", traceback.format_exc())

    def run(self):

        while True:

            pending_tasks = self.pending()  
            if not pending_tasks: 
                time.sleep(1)
                continue

            start_time = time.time()
            logging.info(f"Started working on {len(pending_tasks)} tasks...")

            execute_funcs(self.execute, pending_tasks)
        
            end_time = round(time.time() - start_time, 4)
            logging.info(f"Done in {end_time} {'second' if end_time == 1 else 'seconds'}!")



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
    logging.info("\n\nKERGROUND READY\n\n")
    
    try:
        Kerground(workers_path).run()
    except KeyboardInterrupt:
        logging.info("\n\nKERGROUND STOPPED\n\n")


if __name__ == "__main__": 
    cli()
