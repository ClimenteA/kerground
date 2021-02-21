import time


def long_task():
    time.sleep(5)
    return "Long task finished!" + str(time.time())

def param_task(name):
    raise Exception("test")

def param_many_task(name, age=29):
    time.sleep(1)
    return "Hello " + name + str(age) + str(time.time())



