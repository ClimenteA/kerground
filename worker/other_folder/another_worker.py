import time
import os

def another_task():
    # print("Hot reload for functions registered at init!")
    # time.sleep(4)
    print("DONE pid:", os.getpid())
    return "another_task"
