import os
from crocker import Crocker

worker = Crocker()


def sending_an_event():
    id = worker.send('another_task')
    # res = worker.execute(id)
    # print(res)
    # print(worker.status(id))
    
    
def sending_another_event():
    id = worker.send('long_task')
    # print(id)
    # res = worker.execute(id)
    # print(res)
    # print(worker.status(id))
    


sending_an_event()
# sending_another_event()

# print(worker.finished())
