import sqlite3
import os, shutil
import tempfile
import uuid, pickle
from collections import namedtuple
from importlib.util import spec_from_file_location, module_from_spec
from inspect import getmembers, isfunction

import sys, traceback
import concurrent.futures as cf


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

PendingTask  = namedtuple('PendingTask' , ['id', 'event', 'args', 'status', 'response'])
FinishedTask = namedtuple('FinishedTask' , ['id', 'event', 'args', 'status', 'response'])

worker_funcs = {}

class BGPKWorker:

    def __init__(self, worker_dir=BASE_DIR):
        
        self.worker_dir = BGPKWorker.prep_worker_dir(worker_dir)
        
        BGPKWorker.get_workers()
        
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
    def get_workers():

        for root, dirs, files in os.walk(BASE_DIR): 
            for file in files:
                if (
                    file.endswith("_worker.py")
                    or
                    file.startswith("worker_") and file.endswith(".py")
                ):
                    
                    file_path = os.path.join(root, file)
                    file_id = str(uuid.uuid5(uuid.NAMESPACE_OID, file_path))

                    worker_funcs.update({
                        file_id: {
                            'worker_path': file_path,

                        }
                    })


    def save_task(self, task):
        with open(os.path.join(self.worker_dir, f'{task.id}.pickle'), 'wb') as pkl:
            pickle.dump(task, pkl)  

    def load_task(self, id):
        with open(os.path.join(self.worker_dir, f'{id}.pickle'), 'rb') as pkl:
            task = pickle.load(pkl)
        return task
    
    def send(self, event, *args):
        
        if not isinstance(event, str):
            event = event.__name__

        task = PendingTask(str(uuid.uuid4()), event, args, 'pending', None)
        self.save_task(task)
        self.con.execute("INSERT INTO tasks (id, status) VALUES (?, ?);", (task.id, task.status))
        self.con.commit()
        return task.id

    def status(self, id):
        res = self.con.execute("SELECT status FROM tasks WHERE id = ?", (id,)).fetchone()
        return res[0]


    def execute(self, id):
        
        task = self.load_task(id)
        self.con.execute("UPDATE tasks SET status = ? WHERE id = ?;", ("running", id))
        self.con.commit()

        try:


            # response = task.func(*task.args)
            # ftask = FinishedTask(id, task.func, task.args, 'done', response)

            task.event

            print(task.event)

            # str(uuid.uuid5(uuid.NAMESPACE_OID, worker_file_path)
            # module_name = os.path.basename(worker_file_path).split('.py')[0]
            # spec_module = spec_from_file_location(module_name, worker_file_path)
            # module = module_from_spec(spec_module)
            
            # # Import the module
            # spec_module.loader.exec_module(module)
            # # Execute a function from that module
            # module.another_task()
            # from inspect import getmembers, isfunction
            # list_of_functions = getmembers(module, isfunction)

            
            # self.con.execute("UPDATE tasks SET status = ? WHERE id = ?;", (ftask.status, ftask.id))
            # self.con.commit()
        
            # return response

        except:
            ftask = FinishedTask(id, task.func, task.args, 'failed', traceback.format_exc())
            
            self.con.execute("UPDATE tasks SET status = ? WHERE id = ?;", (ftask.status, ftask.id))
            self.con.commit()

            self.save_task(ftask)




    



worker = BGPKWorker()  


# worker.execute(id)


worker_file_path = "/home/acmt/Documents/background-worker/worker/other_folder/another_worker.py"

# Load
# str(uuid.uuid5(uuid.NAMESPACE_OID, worker_file_path)
module_name = os.path.basename(worker_file_path).split('.py')[0]
spec_module = spec_from_file_location(module_name, worker_file_path)
module = module_from_spec(spec_module)
spec_module.loader.exec_module(module)
func_names = [f[0] for f in getmembers(module, isfunction)]


# spec_module, module, func_names

# Execute
# Import the module
# spec_module.loader.exec_module(module)
# Execute a function from that module
# eval('module.another_task')()


print("list of funcs:", func_names)


# def test_name_func():
#     pass

# print(test_name_func.__name__)