# kerground 
[![Downloads](https://pepy.tech/badge/kerground)](https://pepy.tech/project/kerground) [![PyPI](https://img.shields.io/pypi/v/kerground?color=blue)](https://pypi.org/project/kerground/)


Stupid simple background worker based on python.


## Quickstart

Install

```py
pip install kerground
```

Mark your file workers by naming them with `_worker.py` prefix

```py
my_worker.py
```

Kerground will look in `*_worker.py` and will consider each function an event. 
**Functions from `*_worker.py` files must be unique.**

Import `Kerground`, instantiate it and start sending events:

```py
#my_api.py

from kerground import Kerground

ker = Kerground()

@app.route('/some-task')
def long_wait():
    id = ker.send('long_task') 
    return {'id': id}

```



Event `long_task` is a function name from *_worker.py files
    

```py
#my_worker.py
import time

def long_task():
    # heavy workoad, more than a few seconds job
    time.sleep(2)
    
```


**Your api's and workers must be in the same package/directory**

```bash
root
├── api
│   ├── __init__.py
│   └── my_api.py
└── worker
    ├── __init__.py
    └── my_worker.py
```
You are free to use any folder structure. 


Open 2 cmd/terminal windows in the example directory:
- in one start your api `python3 api/my_api.py`
- in the other one type `kerground`

![kerground_example.gif](pics/kerground_example.gif)


## Multiple Kerground instance and Workers

Optionally, when instantiating the Kerground (or start the worker process from console), you can pass the optional argument `workers_path`, in this case, Kerground will look in `*_worker.py` of each folder passed and will consider each function an event. 
**Functions from `*_worker.py` files must be unique.** 

Due to that, according the path given to `workers_path` parameter Kerworker will create a separated folder with a separated db in the `tempfile` system directory for every instance of Kerworker.

This will allow you to define multiple queue and workers on separated db and manage the processing of those workers separately.

```py
#my_api.py from example_multiple_workers
from kerground import Kerground

pri_ker = Kerground(workers_path="../main_worker")
sec_ker = Kerground(workers_path="../secondary_worker")

@app.route('/main-worker/add-long-task')
def f1():
    id = pri_ker.send('long_task')
    # you will receive an id which you can use howerver you want
    # here we send it to frontend to ask later if task is done
    return {'id': id}

@app.route('/secondary-worker/add-very-long-task')
def f2():
    id = sec_ker.send('long_task')
    # you will receive an id which you can use howerver you want
    # here we send it to frontend to ask later if task is done
    return {'id': id}
    
```

Remember when you will start the workers, you need to give also the parameter `--workers-path`

```sh
#kerground/example_multiple_workers
python3 kerground.py --workers-path='./main_worker'
python3 kerground.py --workers-path='./secondary_worker'
```

```sh
#tmp folder
>> ls -lah
total 0
drwxr-xr-x    4 simone  staff   128B Jul 16 09:28 .
drwx------@ 194 simone  staff   6.1K Jul 16 09:28 ..
drwxr-xr-x    3 simone  staff    96B Jul 16 09:28 main_worker
drwxr-xr-x    3 simone  staff    96B Jul 16 09:28 secondary_worker
```
```sh
#inside main_worker folder
>> ls
tasks.db
a473dcda-d6e0-4fe1-9944-d708148ef1fb.pickle

#inside secondary_worker folder
>> ls
tasks.db
d03237c6-83a9-45c4-a91b-46c6fb2b090c.pickle
```

## API

### `ker.send('func_name', *func_args, timeout=None, purge=True)` 

Send event to kerground worker. `send` function will return the id of the task sent to the worker. 
You have **hot reload** on your workers by default! (as long you don't change function names)

- `timeout`: will show in kerground logs a warning if function takes longer than expected;
- `purge`  : if `True` when function is executed event will be deleted, if `False` event will be deleted after a `ker.get_response(id)` call.


### `ker.status(id)` 

Check status of a task with `status`. Kerground has the folowing statuses:
- pending  - event is added to kerground queue
- running  - event is running
- finished - event was executed succesfully
- failed   - event failed to be executed

Also you can check at any time the statuses of your tasks without specifing the id's:
```py
ker.pending() 
ker.running()
ker.finished()
ker.failed()
```
Or check all statuses with:
```py
ker.stats()
```


### `ker.get_response(id)`

Get the response from event (will be `None` if event didn't ran yet).

You can see functions collected from `*_worker.py` files with:
```py
ker.events
```

## Why

Under the hood kerground uses pickle for serialization of input/output data, a combination of `inspect` methods and built-in `getattr` function for dynamically calling the `"events"`(functions) from `*_worker.py` files. 
It's **resource frendly** (it doesn't use RAM to hold queue), **easy to use** (import kerground, mark your worker files with `_worker.py` prefix and you are set), **has hot reload for workers** (no need to restart workers each time you make a change) **works on multiple cores** (uses multiprocessing).


**Submit any questions/issues you have! Fell free to fork it and improve it!**