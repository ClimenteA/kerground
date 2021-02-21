from worker import worker

def sending_an_event():
    # worker.send(another_task)
    # or 
    id = worker.send('another_task')
    worker.execute(id)
    
    

sending_an_event()