import sqlite3
import os, shutil
import tempfile
import uuid, pickle
from collections import namedtuple

import inspect
import sys
import concurrent.futures as cf



BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

PendingTask  = namedtuple('PendingTask' , ['id', 'event', 'args', 'status', 'response'])
FinishedTask = namedtuple('FinishedTask' , ['id', 'event', 'args', 'status', 'response'])


class BGPKWorker:

    def __init__(self, worker_dir=BASE_DIR):
        self.worker_dir = BGPKWorker.prep_worker_dir(worker_dir)
        self.worker_file_paths = BGPKWorker.get_worker_file_paths()
        self.con = sqlite3.connect(os.path.join(self.worker_dir, 'tasks.db'))
        self.con.execute("CREATE TABLE IF NOT EXISTS tasks (id TEXT NOT NULL, status TEXT NOT NULL);")
        self.con.commit()


    @staticmethod
    def prep_worker_dir(worker_dir):
        
        if not worker_dir: 
            worker_dir = os.path.join(tempfile.gettempdir(), '.BGPKWorker')
        else:
            worker_dir = os.path.join(worker_dir, '.BGPKWorker')

        if os.path.exists(worker_dir): 
            shutil.rmtree(worker_dir)
            os.mkdir(worker_dir)
        else:
            os.mkdir(worker_dir)

        return worker_dir


    @staticmethod
    def get_worker_file_paths():

        worker_file_paths = []   
        for root, dirs, files in os.walk(BASE_DIR): 
            for file in files:
                if (
                    file.endswith("_worker.py")
                    or
                    file.startswith("worker_") and file.endswith(".py")
                ):
                    worker_file_paths.append(os.path.join(root, file))
                    
        return worker_file_paths


    def save_task(self, task):
        with open(os.path.join(self.worker_dir, f'{task.id}.pickle'), 'wb') as pkl:
            pickle.dump(task, pkl)  

    def load_task(self, id):
        with open(os.path.join(self.worker_dir, f'{id}.pickle'), 'rb') as pkl:
            task = pickle.load(pkl)
        return task
    
    def send(self, event, *args):
        task = PendingTask(str(uuid.uuid4()), event, args, 'pending', None)
        self.save_task(task)
        self.con.execute("INSERT INTO tasks (id, status) VALUES (?, ?);", (task.id, task.status))
        self.con.commit()
        return task.id

    def status(self, id):
        res = self.con.execute("SELECT status FROM tasks WHERE id = ?", (id,)).fetchone()
        return res[0]



worker = BGPKWorker()  

worker_file_path = "/home/acmt/Documents/background-worker/worker/other_folder/another_worker.py"



from importlib.util import spec_from_file_location, module_from_spec


module_name = os.path.basename(worker_file_path).split('.py')[0]
spec_module = spec_from_file_location(module_name, worker_file_path)
module = module_from_spec(spec_module)
# Import the module
spec_module.loader.exec_module(module)
# Execute a function from that module
module.another_task()
