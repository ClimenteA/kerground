import os
import time
import uuid
import json
import shutil
import itertools
import traceback
from enum import Enum
from functools import wraps
from threading import Thread
from itertools import groupby
from operator import itemgetter
from multiprocessing import Process, cpu_count


def batch(iterable, size):
    it = iter(iterable)
    while item := list(itertools.islice(it, size)):
        yield item


CPUS = cpu_count()

DASHBOARD_TEMPLATE = """<!doctype html>
<html lang="en">

<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    html {
      font-family: sans-serif;
      color: rgb(33, 33, 33);
    }

    .container-fluid {
      display: grid;
      align-items: center;
      max-width: 1200px;
      margin: 0 auto;
      gap: 2rem;
    }

    table {
      border-collapse: collapse;
    }

    th,
    td {
      padding: 5px;
      border: 2px solid rgb(144, 144, 144);
    }
  </style>
  <title>Kerground Dashboard</title>
</head>

<body>
  <main class="container-fluid">

    <h1>Kerground Tasks</h1>

    <table>

      <thead>
        <tr>
          <th>Task</th>
          <th>Pending</th>
          <th>Running</th>
          <th>Failed</th>
          <th>Done</th>
        </tr>
      </thead>

      <tbody>

        % for i in tasks:

        <tr>
          <td>{{ i['task'] }}</td>
          <td>{{ i['pending'] }}</td>
          <td>{{ i['running'] }}</td>
          <td>{{ i['failed'] }}</td>
          <td>{{ i['done'] }}</td>
        </tr>

        % end

      </tbody>
    </table>
  </main>
</body>


<script>
  document.addEventListener("DOMContentLoaded", () => {
    setInterval(function () { location.reload(); }, 2000);
  }, false);
</script>

</html>
"""


class Modes(str, Enum):
    PROCESS = "process"
    THREAD = "thread"
    SYNC = "sync"


class Status(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    FAILED = "failed"
    DONE = "done"


class Kerground:

    MODE = Modes
    STATUS = Status

    def __init__(self, tasks_path: str = "./.kergroundtasks", pool: int = 1):
        self.tasks = {}
        self.pool = pool
        self.tasks_path = tasks_path
        self.pending_path = os.path.join(tasks_path, "pending")
        self.running_path = os.path.join(tasks_path, "running")
        self.done_path = os.path.join(tasks_path, "done")
        self.failed_path = os.path.join(tasks_path, "failed")
        self.all_paths = [
            self.pending_path,
            self.running_path,
            self.done_path,
            self.failed_path,
        ]
        for path in self.all_paths:
            try:
                os.makedirs(path)
            except:
                pass

    def register(self, *dargs, mode: Modes = Modes.PROCESS, max_retries: int = 0):
        def decorator(fn):
            self.tasks[fn.__name__] = {
                "task": fn,
                "mode": mode,
                "max_retries": max_retries,
            }

            @wraps(fn)
            def wrapper(*args, **kwargs):
                return fn(*args, **kwargs)

            return wrapper

        return decorator(dargs[0]) if dargs and callable(dargs[0]) else decorator

    def enqueue(self, fn: str, *args, **kwargs):
        task = fn if isinstance(fn, str) else fn.__name__
        assert task in self.tasks, "this task is not registered"

        message_id = str(uuid.uuid4())
        task_path = os.path.join(self.pending_path, f"{task}--{message_id}.json")
        message = {
            "message_id": message_id,
            "task": task,
            "mode": self.tasks[task]["mode"],
            "max_retries": self.tasks[task]["max_retries"],
            "args": args,
            "kwargs": kwargs,
        }
        with open(task_path, mode="w") as jsonfile:
            json.dump(message, jsonfile)

        return message_id

    def check_status(self, message_id: str):
        for status in ["pending", "running", "failed"]:
            for task in os.listdir(os.path.join(self.tasks_path, status)):
                if message_id in task:
                    return status
        return "done"

    def load_jobs(self, status: str):
        assert status in ["pending", "running", "failed", "done"]
        jobs = []
        jobs_path = os.path.join(self.tasks_path, status)
        for path in [os.path.join(jobs_path, f) for f in os.listdir(jobs_path)]:
            with open(path, "r") as jsonfile:
                message = json.load(jsonfile)
                jobs.append(message)
        return jobs

    def move(self, message_id: str, old: str, new: str):
        assert old in ["pending", "running", "failed", "done"]
        assert new in ["pending", "running", "failed", "done"]
        src = os.path.join(self.tasks_path, old, f"{message_id}.json")
        dst = os.path.join(self.tasks_path, new, f"{message_id}.json")
        shutil.move(src, dst)

    def run(self, fn: str, message_id: str, max_retries: int, *args, **kwargs):
        try:
            print(f"Working on '{fn}--{message_id}'...")
            start = time.perf_counter()
            result = self.tasks[fn]["task"](*args, **kwargs)
            self.move(f"{fn}--{message_id}", "running", "done")
            print(
                f"Successfully finished '{fn}-{message_id}' in {time.perf_counter() - start} seconds!"
            )
            return result
        except Exception:
            print(traceback.format_exc())
            print(f"Failed executing '{fn}-{message_id}'...")
            if max_retries > 0:
                print("Retrying...")
                time.sleep(5)
                max_retries -= 1
                return self.run(fn, message_id, max_retries, *args, **kwargs)
            self.move(f"{fn}--{message_id}", "running", "failed")

    def run_sync(self, fn, tasks_sync):
        for t in tasks_sync:
            self.run(fn, t["message_id"], t["max_retries"], *t["args"], *t["kwargs"])

    def run_processes(self, fn, tasks_processes):

        procs = []
        for t in tasks_processes:
            p = Process(
                target=self.run,
                args=(fn, t["message_id"], t["max_retries"], *t["args"], *t["kwargs"]),
            )
            p.start()
            procs.append(p)
        for proc in procs:
            proc.join()

    def run_threads(self, fn, tasks_threads):
        threads = []
        for t in tasks_threads:
            t = Thread(
                target=self.run,
                args=(fn, t["message_id"], t["max_retries"], *t["args"], *t["kwargs"]),
            )
            t.start()
            threads.append(t)
        for thread in threads:
            thread.join()

    def work(self):

        pending_jobs = self.load_jobs("pending")
        if not pending_jobs:
            time.sleep(self.pool)

        for key, value in groupby(pending_jobs, key=itemgetter("task")):

            tasks = [k for k in value]
            tasks_processes = [t for t in tasks if t["mode"] == Modes.PROCESS]
            tasks_threads = [t for t in tasks if t["mode"] == Modes.THREAD]
            tasks_sync = [t for t in tasks if t["mode"] == Modes.SYNC]

            print(f"Started '{key}' with {len(tasks)} tasks")

            for t in tasks:
                self.move(f"{t['task']}--{t['message_id']}", "pending", "running")

            if tasks_sync:
                self.run_sync(key, tasks_sync)

            if tasks_processes:
                for b in list(batch(tasks_processes, CPUS)):
                    self.run_processes(key, b)

            if tasks_threads:
                for b in list(batch(tasks_threads, CPUS * 2)):
                    self.run_threads(key, b)

    def listen(self):
        print("Listening...")
        while True:
            try:
                self.work()
            except KeyboardInterrupt:
                print("Stopped")
                break

    def count(self, tasks):
        counttasks = {}
        for t in tasks:
            if t not in counttasks:
                counttasks[t] = 0
            counttasks[t] = counttasks[t] + 1
        return counttasks

    def get_current_tasks(self):

        pending = self.count([i.split("--")[0] for i in os.listdir(self.pending_path)])
        running = self.count([i.split("--")[0] for i in os.listdir(self.running_path)])
        failed = self.count([i.split("--")[0] for i in os.listdir(self.failed_path)])
        done = self.count([i.split("--")[0] for i in os.listdir(self.done_path)])

        tasks = []
        for task in self.tasks.keys():
            t = {
                "task": task,
                "pending": pending.get(task, 0),
                "running": running.get(task, 0),
                "failed": failed.get(task, 0),
                "done": done.get(task, 0),
            }
            tasks.append(t)

        return tasks

    def dashboard(
        self, host: str = "localhost", port: int = 3030, return_bottle: bool = False
    ):
        try:
            import bottle as b
        except:
            print("Please install `bottle` to use the dashboard")
            return

        if not return_bottle:
            has_waitress = False
            try:
                from waitress import serve

                has_waitress = True
            except:
                print("Please install `waitress` for more performance")

        @b.get("/")
        def index():
            return b.template(DASHBOARD_TEMPLATE, tasks=self.get_current_tasks())

        if return_bottle:
            return b

        if has_waitress:
            print("Running with waitress...")
            serve(b.default_app(), host=host, port=port)
        else:
            print("Running with bottle development server...")
            b.run(host=host, port=port)
