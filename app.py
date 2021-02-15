import time
from worker import BackgroundWorker

worker = BackgroundWorker()

start_time = time.time()


def long_task():
    time.sleep(5)
    return "Long task finished!" + str(time.time())

def param_task(name):
    raise Exception("test")
    return "Hello " + name + str(time.time())

def param_many_task(name, age=29):
    time.sleep(1)
    return "Hello " + name + str(age) + str(time.time())


# import inspect

# print(f"""
# {long_task}
# {dir(long_task)}
# {inspect.FrameInfo}
# """)



id1 = worker.add(long_task)
id2 = worker.add(param_task, "Alin")
id3 = worker.add(param_many_task, "Alin", 30)

# res1 = worker.execute(id1)
# print("res1 ", res1)

# res2 = worker.execute(id2)
# print("res2 ", res2)

# res3 = worker.execute(id3)
# print("res3 ", res3)

i = 0
while True:
    
    print("Checking for statuses:", i)

    print(worker.status(id1))
    print(worker.status(id2))
    print(worker.status(id3))
    time.sleep(2)
    i += 1









# import time
# from flask import Flask
# from worker import BackgroundWorker

# app = Flask(__name__)
# worker = BackgroundWorker()


# def long_task():
#     time.sleep(5)
#     return "Long task finished!" + str(time.time())


# @app.route('/')
# def bgtask():
#     return long_task()


# @app.route('/ready')
# def bgtask_ready():
#     return long_task()




# if __name__ == '__main__':
#     app.run(debug=True)