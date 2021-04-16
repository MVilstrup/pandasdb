import concurrent
from concurrent.futures import ThreadPoolExecutor


class Pool:
    pool = ThreadPoolExecutor(50)

    @classmethod
    def execute(cls, job, *args, **kwargs):
        return cls.pool.submit(lambda f: f(*args, **kwargs), job)

    @classmethod
    def map_wait(cls, func, arg_list):
        jobs = [cls.execute(func, arg) for arg in arg_list]
        return cls.wait_for(*jobs)

    @classmethod
    def wait_for(cls, *jobs):
        raise NotImplementedError()

    @classmethod
    def handle(cls, *jobs, **named_jobs):
        assert any([jobs, named_jobs]) and not all([jobs, named_jobs])

        if jobs:
            names, jobs = [], jobs[0] if (len(jobs) == 1 and isinstance(jobs[0], (list, tuple))) else jobs
        else:
            names, jobs = zip(*named_jobs.items())

        futures = [cls.execute(job) for job in jobs]
        results = list(cls.wait_for(*futures))

        return {name: result for name, result in zip(names, results)} if names else results


class Async(Pool):

    @classmethod
    def wait_for(cls, *jobs):
        return [job.result() for job in jobs]


class AsyncTQDM(Pool):

    @classmethod
    def wait_for(cls, *jobs):
        from tqdm.notebook import tqdm
        job_ids = {job: i for i, job in enumerate(jobs)}
        result_ids = {}

        for job in tqdm(concurrent.futures.as_completed(job_ids), total=len(jobs)):
            result_ids[job_ids[job]] = job.result()

        return [result_ids[idx] for idx in sorted(job_ids.values())]


def as_async_map(func):
    from pandasdb.libraries.utils import iterable

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
