from flask import Flask
from waitress import serve

from huey.api import Task
from hueyqueue import add_task, logger

app = Flask(__name__)
app.config['DEBUG'] = True


@app.route('/')
def index():
    logger.info("index: accessing /")
    return 'Hello world'


@app.route('/task')
def register_task():
    logger.info("register_task: accessing /task")
    task: Task = add_task.schedule(args=(), delay=0.1)
    return str(task.id)


@app.route('/moretasks/<int:count>')
def register_tasks(count: int):
    logger.info(f"register_tasks: accessing /moretasks/{count}")
    tasklist = []
    for i in range(count):
        task: Task = add_task.schedule(args=(), delay=0.1)
        tasklist.append(str(task.id))
    return tasklist


if __name__ == '__main__':
    serve(app, host='127.0.0.1', port=5001, threads=2)