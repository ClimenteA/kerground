import sqlite3
import os, shutil
import tempfile
import uuid, pickle
from collections import namedtuple
import concurrent.futures as cf


PendingTask  = namedtuple('PendingTask' , ['id', 'event', 'args', 'status', 'response'])
FinishedTask = namedtuple('FinishedTask' , ['id', 'event', 'args', 'status', 'response'])


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


print(BASE_DIR)


class BGPKWorker:

    def __init__(self, worker_dir=None):
        self.worker_dir = BGPKWorker.prep_worker_dir(worker_dir)
        self.con = sqlite3.connect(os.path.join(self.worker_dir, 'tasks.db'))
        self.con.execute("CREATE TABLE IF NOT EXISTS tasks (id TEXT NOT NULL, status TEXT NOT NULL);")
        self.con.commit()


    @staticmethod
    def prep_worker_dir(worker_dir):
        
        if not worker_dir: 
            worker_dir = os.path.join(tempfile.gettempdir(), 'BGPKWorker')

        if os.path.exists(worker_dir): 
            shutil.rmtree(worker_dir)
            os.mkdir(worker_dir)
        else:
            os.mkdir(worker_dir)

        return worker_dir


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



            
        