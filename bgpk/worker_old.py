import time
import traceback
import sys, sqlite3
import os, uuid, pickle, logging
from collections import namedtuple
import concurrent.futures as cf

# logging.basicConfig(filename='worker.log', level=logging.INFO, format='%(asctime)s - [%(levelname)s][%(name)s] - %(message)s')
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - [%(levelname)s][%(name)s] - %(message)s')


Task = namedtuple('Task' , ['id', 'func', 'args', 'status', 'response'])
FinishedTask = namedtuple('FinishedTask' , ['id', 'func', 'args', 'status', 'response'])
               
class BackgroundWorker:

    def __init__(self, worker_dir="worker_dir"):        
        self.worker_dir = worker_dir
        if not os.path.exists(self.worker_dir): os.mkdir(self.worker_dir)
        self.con = sqlite3.connect(os.path.join(self.worker_dir, 'tasks.db'))
        self.con.execute("CREATE TABLE IF NOT EXISTS tasks (id TEXT NOT NULL, status TEXT NOT NULL);")
        self.con.commit()

    def save_task(self, task):
        
        with open(os.path.join(self.worker_dir, f'{task.id}.pickle'), 'wb') as pkl:
            pickle.dump(task, pkl)  
        return task.id

    def load_task(self, id):
        with open(os.path.join(self.worker_dir, f'{id}.pickle'), 'rb') as pkl:
            task = pickle.load(pkl)
        return task
    
    
    def add(self, func, *args):

        task = Task(str(uuid.uuid4()), func, args, 'pending', None)

        self.con.execute("INSERT INTO tasks (id, status) VALUES (?, ?);", (task.id, task.status))
        self.con.commit()

        return self.save_task(task)


    def status(self, id):
        res = self.con.execute("SELECT status FROM tasks WHERE id = ?", (id,)).fetchone()
        return res[0]

    def execute(self, id):

        task = self.load_task(id)
        self.con.execute("UPDATE tasks SET status = ? WHERE id = ?;", ("running", id))
        self.con.commit()

        try:
            response = task.func(*task.args)
            ftask = FinishedTask(id, task.func, task.args, 'done', response)
            
            self.con.execute("UPDATE tasks SET status = ? WHERE id = ?;", (ftask.status, ftask.id))
            self.con.commit()
        
            return response

        except:
            ftask = FinishedTask(id, task.func, task.args, 'failed', traceback.format_exc())
            
            self.con.execute("UPDATE tasks SET status = ? WHERE id = ?;", (ftask.status, ftask.id))
            self.con.commit()

            self.save_task(ftask)


    def watch(self):
             
        # logging.info(f'Working on task: {task.id}')
        # logging.info(f'Task saved: {task.id}')
        # logging.info(f'New task added: {task.id}')
        # logging.error(f'Task failed: {task.id}')
        # logging.info(f'Task finished processing: {task.id}')

        while True:
            todos = self.con.execute("SELECT id FROM tasks WHERE status = 'pending';").fetchall()
            if todos: tasks = (task[0] for task in todos)
            if not todos: tasks = None
            
            if tasks:
                logging.info('New tasks added')
                for task in tasks:
                    logging.info(f'Working on task: {task}')
                    self.execute(task)
                    logging.info(f'Task finished processing: {task}')


           
            time.sleep(1)

        

    


        # # We can use a with statement to ensure threads are cleaned up promptly
        # with cf.ThreadPoolExecutor(max_workers=5) as executor:
        #     # Start the load operations and mark each future with its URL
        #     future_to_url = {executor.submit(load_url, url, 60): url for url in URLS}
        #     for future in cf.as_completed(future_to_url):
        #         url = future_to_url[future]
        #         try:
        #             data = future.result()
        #         except Exception as exc:
        #             print('%r generated an exception: %s' % (url, exc))
        #         else:
        #             print('%r page is %d bytes' % (url, len(data)))







        # while True: 
            
        #     current_dir_size = dir_size(self.worker_dir)
            
        #     if current_dir_size != initial_dir_size:
        #         initial_dir_size = current_dir_size

        #         with ProcessPoolExecutor() as ppex:

        #             with ThreadPoolExecutor() as tpe:


        #             results = ppex.map(square, values)

        #     time.sleep(0.01)




# TESTS


if __name__ == '__main__':    
    
    if 'watch' in sys.argv:    
        logging.info("Watching for changes...")
        worker = BackgroundWorker()
        worker.watch()
        worker.con.close()
        logging.info("Exited.")
    else:
        logging.error("Please type 'python3 worker.py watch' to start.")









# start_time = time.time()


# def long_task():
#     # time.sleep(5)
#     return "Long task finished!" + str(time.time())

# def param_task(name):
#     raise Exception("test")
#     return "Hello " + name + str(time.time())

# def param_many_task(name, age=29):
#     # time.sleep(1)
#     return "Hello " + name + str(age) + str(time.time())




# id1 = worker.add(long_task)
# id2 = worker.add(param_task, "Alin")
# id3 = worker.add(param_many_task, "Alin", 30)

# res1 = worker.execute(id1)
# print("res1 ", res1)

# res2 = worker.execute(id2)
# print("res2 ", res2)

# res3 = worker.execute(id3)
# print("res3 ", res3)


# print(worker.status(id1))
# print(worker.status(id2))
# print(worker.status(id3))



# print("--- %s seconds ---" % (time.time() - start_time))

