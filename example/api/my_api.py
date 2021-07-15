from flask import Flask
from kerground import Kerground 

ker = Kerground()

app = Flask(__name__)

@app.route('/')
def index():
    route_list = [str(rule) for rule in app.url_map.iter_rules()]
    return {"routes": route_list}

@app.route('/add-long-task')
def f1():
    id = ker.send('long_task')
    # you will receive an id which you can use howerver you want
    # here we send it to frontend to ask later if task is done
    return {'id': id}


@app.route('/task-no-params')
def f2():
    id = ker.send('task_no_params')
    return {'id': id}


@app.route('/task-with-params/<param1>/<param2>')
def f3(param1, param2):
    id = ker.send('task_with_params', param1, param2)
    return {'id': id}


@app.route('/status')
@app.route('/status/<id>')
def f4(id=None):
    
    if id:
        return {'status': ker.status(id)}

    return {
        'pending': ker.pending(),
        'running': ker.running(),
        'finished': ker.finished(),
        'failed': ker.failed()
    }


@app.route('/get-response/<id>')
def f5(id):
    res = ker.get_response(id)
    return {'response': res}



if __name__ == '__main__':
    app.run(debug=True)