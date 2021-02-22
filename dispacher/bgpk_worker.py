import sqlite3
import os, shutil
import tempfile
import uuid, pickle
import itertools, traceback
from collections import namedtuple
from importlib.util import spec_from_file_location, module_from_spec
from inspect import getmembers, isfunction


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

PendingTask  = namedtuple('PendingTask' , ['id', 'event', 'args', 'status', 'response'])
FinishedTask = namedtuple('FinishedTask' , ['id', 'event', 'args', 'status', 'response'])


class BGPKWorker:
    """
        How it works:
        - Initialize this class
        `from bgpk import BGPKWorker`
        `worker = BGPKWorker()`
    
        Events are string function names
        `worker.send('func_name')` will append func to background processing

    """

    def __init__(self, worker_dir=BASE_DIR, persist_data=True, _skip_dirs_creation=False):
        
        # TODO add timeout's, cron_jobs
        self.worker_dir = BGPKWorker.prep_worker_dir(worker_dir, persist_data, _skip_dirs_creation)
        self.events = self.gather_events()
        self.con = sqlite3.connect(os.path.join(self.worker_dir, 'tasks.db'))
        self.con.execute("CREATE TABLE IF NOT EXISTS tasks (id TEXT NOT NULL, status TEXT NOT NULL);")
        self.con.commit()

    # @property - works with init values...
    @classmethod    
    def pending(cls): 
        return cls().tasks_by_status('pending')
    
    @classmethod    
    def running(cls): 
        return cls().tasks_by_status('running')

    @classmethod
    def finished(cls): 
        return cls().tasks_by_status('finished')

    @classmethod
    def failed(cls): 
        return cls().tasks_by_status('failed')
        

    def tasks_by_status(self, status):
        res = self.con.execute("SELECT id FROM tasks WHERE status = ?", (status,)).fetchall()
        res = [r[0] for r in res]
        return res

    @staticmethod
    def prep_worker_dir(worker_dir, persist_data, _skip_dirs_creation):
        
        if _skip_dirs_creation:
            return os.environ.get("BGPK_worker_dir")

        if not worker_dir: 
            worker_dir = os.path.join(tempfile.gettempdir(), '.BGPKWorker')
        else:
            worker_dir = os.path.join(worker_dir, '.BGPKWorker')

        if not persist_data:
            shutil.rmtree(worker_dir)
            os.mkdir(worker_dir)
        
        if not os.path.exists(worker_dir):
            os.mkdir(worker_dir)

        os.environ["BGPK_worker_dir"] = worker_dir

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
                    spec, module, func_names = BGPKWorker.get_module_data(file_path)
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

            self.con.execute("UPDATE tasks SET status = ? WHERE id = ?;", (ftask.status, ftask.id))
            self.con.commit()

            return response

        except:

            ftask = FinishedTask(id, task.event, task.args, 'failed', traceback.format_exc())
            
            self.con.execute("UPDATE tasks SET status = ? WHERE id = ?;", (ftask.status, ftask.id))
            self.con.commit()

            self.save_task(ftask)


