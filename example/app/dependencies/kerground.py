import os
import time
import uuid
import json
import shutil
import traceback
from enum import Enum
from functools import wraps
from threading import Thread
from itertools import groupby
from operator import itemgetter
from multiprocessing import Process


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
            if not os.path.exists(path):
                os.makedirs(path)

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
        task_path = os.path.join(self.pending_path, f"{message_id}.json")
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
        for status in ["pending", "running", "failed", "done"]:
            for task in os.listdir(os.path.join(self.tasks_path, status)):
                if message_id in task:
                    return status

    def load_jobs(self, status: str):
        assert status in ["pending", "running", "failed", "done"]
        jobs = []
        jobs_path = os.path.join(self.tasks_path, status)
        for path in [os.path.join(jobs_path, f) for f in os.listdir(jobs_path)]:
            with open(path, "r") as jsonfile:
                message = json.load(jsonfile)
                jobs.append(message)
        return jobs

    def pending(self):
        return self.load_jobs("pending")

    def running(self):
        return self.load_jobs("running")

    def failed(self):
        return self.load_jobs("failed")

    def done(self):
        return self.load_jobs("done")

    def move(self, message_id: str, old: str, new: str):
        assert old in ["pending", "running", "failed", "done"]
        assert new in ["pending", "running", "failed", "done"]
        src = os.path.join(self.tasks_path, old, f"{message_id}.json")
        dst = os.path.join(self.tasks_path, new, f"{message_id}.json")
        shutil.move(src, dst)

    def run(self, fn: str, message_id: str, max_retries: int, *args, **kwargs):
        try:
            print(f"Working on '{fn}-{message_id}'...")
            start = time.perf_counter()
            result = self.tasks[fn]["task"](*args, **kwargs)
            self.move(message_id, "running", "done")
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
            self.move(message_id, "running", "failed")

    def run_sync(self, fn, tasks_sync):
        for t in tasks_sync:
            self.run(fn, t["message_id"], t["max_retries"], *t["args"], *t["kwargs"])

    def run_cpu_bound(self, fn, tasks_cpu_bound):
        procs = []
        for t in tasks_cpu_bound:
            p = Process(
                target=self.run,
                args=(fn, t["message_id"], t["max_retries"], *t["args"], *t["kwargs"]),
            )
            p.start()
            procs.append(p)
        for proc in procs:
            proc.join()

    def run_io_bound(self, fn, tasks_io_bound):
        threads = []
        for t in tasks_io_bound:
            t = Thread(
                target=self.run,
                args=(fn, t["message_id"], t["max_retries"], *t["args"], *t["kwargs"]),
            )
            t.start()
            threads.append(t)
        for thread in threads:
            thread.join()

    def work(self):

        pending_jobs = self.pending()
        if not pending_jobs:
            time.sleep(self.pool)

        for key, value in groupby(pending_jobs, key=itemgetter("task")):

            tasks = [k for k in value]
            tasks_cpu_bound = [t for t in tasks if t["mode"] == Modes.PROCESS]
            tasks_io_bound = [t for t in tasks if t["mode"] == Modes.THREAD]
            tasks_sync = [t for t in tasks if t["mode"] == Modes.SYNC]

            print(f"Started '{key}' with {len(tasks)} tasks")

            for t in tasks:
                self.move(t["message_id"], "pending", "running")

            if tasks_sync:
                self.run_sync(key, tasks_sync)

            if tasks_cpu_bound:
                self.run_cpu_bound(key, tasks_cpu_bound)

            if tasks_io_bound:
                self.run_io_bound(key, tasks_io_bound)

    def listen(self):
        print("Listening...")
        while True:
            try:
                self.work()
            except KeyboardInterrupt:
                print("Stopped")
                break


ker = Kerground()
