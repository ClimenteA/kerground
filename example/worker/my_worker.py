import time


def long_task():
    time.sleep(0.1)
    return


def task_no_params():
    time.sleep(1)
    return

def task_with_params(param1, param2="default"):
    
    with open("some.txt", 'w') as f:
        f.write(str(param1 + param2))
    
    return param1, param2

