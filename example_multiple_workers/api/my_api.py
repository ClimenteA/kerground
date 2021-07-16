from flask import Flask
from kerground import Kerground

pri_ker = Kerground(workers_path="../main_worker")
sec_ker = Kerground(workers_path="../secondary_worker")

app = Flask(__name__)

@app.route('/')
def index():
    route_list = [str(rule) for rule in app.url_map.iter_rules()]
    return {"routes": route_list}

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


@app.route('/main-worker/status')
@app.route('/main-worker/status/<id>')
def f3(id=None):
    
    if id:
        return {'status': pri_ker.status(id)}

    return {
        'pending': pri_ker.pending(),
        'running': pri_ker.running(),
        'finished': pri_ker.finished(),
        'failed': pri_ker.failed()
    }

@app.route('/secondary-worker/status')
@app.route('/secondary-worker/status/<id>')
def f4(id=None):
    
    if id:
        return {'status': sec_ker.status(id)}

    return {
        'pending': sec_ker.pending(),
        'running': sec_ker.running(),
        'finished': sec_ker.finished(),
        'failed': sec_ker.failed()
    }

@app.route('/main-worker/get-response/<id>')
def f5(id):
    res = pri_ker.get_response(id)
    return {'response': res}


@app.route('/secondary-worker/get-response/<id>')
def f6(id):
    res = sec_ker.get_response(id)
    return {'response': res}


if __name__ == '__main__':
    app.run(debug=True)