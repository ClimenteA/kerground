from flask import Flask

from kerground import ker


app = Flask(__name__)


@app.route('/')
def index():
    route_list = [str(rule) for rule in app.url_map.iter_rules()]
    return {"routes": route_list}


@app.route('/add-long-task')
def long_task_adder():
    id = ker.send('long_task', "params")
    # you will receive an id which you can use howerver you want
    # here we send it to frontend to ask later if task is done
    return {'id': id}


@app.route('/status')
@app.route('/status/<id>')
def long_task_status(id=None):
    
    if id:
        return {'status': ker.status(id)}

    return {
        'pending': ker.pending(),
        'running': ker.running(),
        'finished': ker.finished(),
        'failed': ker.failed()
    }




if __name__ == '__main__':
    app.run(debug=True)