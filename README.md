# kerground 
[![Downloads](https://pepy.tech/badge/kerground)](https://pepy.tech/project/kerground) [![PyPI](https://img.shields.io/pypi/v/kerground?color=blue)](https://pypi.org/project/kerground/)

Background worker based on pickle and sqlite3.


## Quickstart

**Install**

```py
pip install kerground
```


**Mark your file workers by naming them with `_worker.py` prefix**
```py
my_worker.py
```
Kerground will look in `*_worker.py` and will consider each function an event (similar to pytest).

**Import `kerground` instance in your api/views/dispacher and start sending events**

```py
from kerground import ker

@app.route('/some-task')
def long_wait():
    id = ker.send('long_task')
                # ^^^^^^^^^^^ this is a function name from *_worker.py files
    return {'id': id}

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


**Open 2 cmd/terminal windows in the example directory:**
- in one start your api `python3 api/my_api.py`
- in the other one type `kerground`

![kerground_example.gif](pics/kerground_example.gif)

You are free to use any folder structure. 


## API


### `ker.send('func_name', *args)` 

Send event to kerground worker (you can also import the function from worker file and pass it to send method). `send` function will return the id of the task sent to the worker. 

You have **hot reload** on your workers by default! (`send` calls the function dinamically from worker files).


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

### `ker.get_response(id)`

Get the response from event (will be `None` if event didn't ran yet).

You can see functions collected from `*_worker.py` files with:
```py
ker.events_collected
```

## Why

Under the hood kerground uses pickle for serialization of input/output data, a combination of `inspect` methods and built-in `getattr` function for dynamically calling the `"events"`(functions) from `*_worker.py` files. 
It's **resource frendly** (it doesn't use RAM to hold queue), **easy to use** (import kerground, mark your worker files with `_worker.py` prefix and you are set), **has hot reload for workers** (no need to restart workers each time you make a change).


**Submit any questions/issues you have! Fell free to fork it and improve it!**