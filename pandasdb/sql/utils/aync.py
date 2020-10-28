from concurrent.futures import ThreadPoolExecutor
from multiprocessing import cpu_count


class Async:
    pool = ThreadPoolExecutor(cpu_count())

    @staticmethod
    def execute(job, *args, **kwargs):
        return Async.pool.submit(lambda f: f(*args, **kwargs), job)

    @staticmethod
    def wait_for(*jobs):
        for job in jobs:
            yield job.result()

    @staticmethod
    def handle(*jobs):
        futures = [Async.execute(job) for job in jobs]
        return list(Async.wait_for(*futures))
