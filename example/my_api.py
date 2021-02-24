from flask import Flask
from kerground import Kerground

k = Kerground()

app = Flask(__name__)


@app.route('/')
def index():
    route_list = [str(rule) for rule in app.url_map.iter_rules()]
    return {"routes": route_list}


@app.route('/long-task')
def long_task_adder():
    id = k.send('long_task', "params")
    # you will receive an id which you can use howerver you want
    # here we send it to frontend to ask later if task is done
    return {'id': id}


@app.route('/status/<id>')
def long_task_status(id):
    return {'status': k.status(id)}


@app.route('/tasks-stats')
def tasks_stats():
    return {'stats': {
        'pending': k.pending(),
        'running': k.running(),
        'finished': k.finished(),
        'failed': k.failed()
    }}



if __name__ == '__main__':
    app.run(debug=True)