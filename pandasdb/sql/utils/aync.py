import concurrent
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import cpu_count


class Pool:
    pool = ThreadPoolExecutor(cpu_count())


class Async(Pool):

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
    def handle(*jobs, **named_jobs):
        assert not (jobs and named_jobs)
        if jobs:
            names = []
            if len(jobs) == 1 and isinstance(jobs[0], list):
                jobs = jobs[0]
        else:
            names, jobs = zip(named_jobs.items())

        futures = [Async.execute(job) for job in jobs]
        results = list(Async.wait_for(*futures))

        if names:
            return {name: result for name, result in zip(names, results)}
        else:
            return results


class AsyncTQDM(Pool):

    @staticmethod
    def execute(job, *args, **kwargs):
        return AsyncTQDM.pool.submit(lambda f: f(*args, **kwargs), job)

    @staticmethod
    def wait_for(*jobs):
        from tqdm.notebook import tqdm
        job_ids = {job: i for i, job in enumerate(jobs)}
        result_ids = {}

        for job in tqdm(concurrent.futures.as_completed(job_ids), total=len(jobs)):
            result_ids[job_ids[job]] = job.result()

        for i in sorted(job_ids.values()):
            yield result_ids[i]

    @staticmethod
    def map_wait(func, arg_list):
        jobs = [AsyncTQDM.execute(func, arg) for arg in arg_list]
        return AsyncTQDM.wait_for(*jobs)

    @staticmethod
    def handle(*jobs, **named_jobs):
        assert not (jobs and named_jobs)
        if jobs:
            names = []
            if len(jobs) == 1 and isinstance(jobs[0], list):
                jobs = jobs[0]
        else:
            names, jobs = zip(*named_jobs.items())

        futures = [AsyncTQDM.execute(job) for job in jobs]
        results = list(AsyncTQDM.wait_for(*futures))

        if names:
            return {name: result for name, result in zip(names, results)}
        else:
            return results


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
