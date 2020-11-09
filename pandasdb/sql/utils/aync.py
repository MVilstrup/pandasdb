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
    def map_wait(func, arg_list):
        jobs = [Async.execute(func, arg) for arg in arg_list]
        return Async.wait_for(*jobs)

    @staticmethod
    def handle(*jobs):
        if len(jobs) == 1 and isinstance(jobs[0], list):
            jobs = jobs[0]

        futures = [Async.execute(job) for job in jobs]
        return list(Async.wait_for(*futures))


def as_async_map(func):
    from pandasdb.sql.utils import iterable

    class AsyncFunc:
        def __init__(self, func):
            self.func = func

        def apply(self, args):
            if not iterable(args):
                self._error()

            return Async.map_wait(func, args)

        def map(self, args):
            return self.apply(args)

        def _error(self):
            # @no:format
            raise Exception(f"{self.func.__name__} has been converted to an async map function, and should be given a list of elements")
            # @do:format

        def __call__(self, *args, **kwargs):
            if len(args) == 1 and iterable(args[0]):
                return self.apply(args[0])

            return self.apply(args)

    return AsyncFunc(func)
