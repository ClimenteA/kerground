import time


def long_task():
    # very heavy workoad, more than a few seconds job
    time.sleep(10)
    return "done"


def task_one_param(param):
    # time.sleep(0.2)
    return param


def task_no_params():
    # time.sleep(0.2)
    return


def task_with_params(param1, param2):
    time.sleep(2)
    return param1, param2

