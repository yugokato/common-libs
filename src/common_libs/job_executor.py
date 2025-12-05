import multiprocessing
from collections.abc import Callable, Sequence
from concurrent import futures
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, cast


@dataclass(frozen=True, slots=True)
class Job:
    func: Callable[..., Any]
    args: tuple[Any, ...] = field(default=())
    kwargs: dict[str, Any] = field(default_factory=dict)


def run_concurrent(
    jobs: Sequence[Job], max_workers: int | None = None, is_subprocess: bool = False, return_exceptions: bool = False
) -> list[Any]:
    """Run function concurrently using multiple threads

    NOTE: Make sure to specify is_subprocess=True when a job spawns a subprocess to cap the num workers within num CPUs,
          especially when processing large number of jobs.

    :param jobs: Jobs to run concurrently
    :param max_workers: Max workers
    :param is_subprocess: Jobs were spawned as subprocesses
    :param return_exceptions:  If a job throws an exception, return the exception object as a Job result instead
    """
    results = []
    with ThreadPoolExecutor(
        max_workers=_get_max_workers(len(jobs), max_workers, limit_by_num_cpu=is_subprocess)
    ) as executor:
        _futures = [executor.submit(job.func, *job.args, **job.kwargs) for job in jobs]
    try:
        for future in futures.as_completed(_futures):
            try:
                results.append(future.result())
            except Exception as e:
                if return_exceptions:
                    results.append(e)
                else:
                    raise
    except KeyboardInterrupt:
        # Force shutdown jobs
        # Reference: https://gist.github.com/clchiou/f2608cbe54403edb0b13
        print("Aborted")  # noqa: T201
        executor._threads.clear()  # type: ignore[attr-defined]
        futures.thread._threads_queues.clear()  # type: ignore[attr-defined]
        raise

    return results


def run_parallel(jobs: Sequence[Job], max_workers: int | None = None) -> list[Any]:
    """Run function in parallel using multiple processes

    NOTE: This can be used only in __main__. Use this in a general python script

    :param jobs: Jobs to run in parallel
    :param max_workers: Max workers
    """
    results = []
    try:
        with ProcessPoolExecutor(max_workers=_get_max_workers(len(jobs), max_workers)) as e:
            _futures = [e.submit(job.func, *job.args, **job.kwargs) for job in jobs]
            for future in futures.as_completed(_futures):
                results.append(future.result())
    except KeyboardInterrupt:
        print("Aborted")  # noqa: T201

    return results


def _get_max_workers(len_jobs: int, desired_num_workers: int | None, limit_by_num_cpu: bool = True) -> int:
    """Identify the actual max workers the node can/should spawn"""
    assert len_jobs > 0
    possible_num_workers = [(desired_num_workers or float("inf")), len_jobs]
    if limit_by_num_cpu:
        possible_num_workers.append(multiprocessing.cpu_count())
    return cast(int, min(possible_num_workers))
