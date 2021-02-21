from worker import worker

def sending_an_event():
    # worker.send(another_task)
    # or 
    id = worker.send('another_task')
    res = worker.execute(id)
    print(res)
    
    

sending_an_event()