"""Tests for common_libs.job_executor module"""

import multiprocessing
import time

import pytest
from pytest_mock import MockFixture

from common_libs.job_executor import Job, run_concurrent


def simple_func(x: int) -> int:
    """Simple test function"""
    return x * 2


def slow_func(x: int) -> int:
    """Slow test function"""
    time.sleep(0.1)
    return x


def failing_func() -> None:
    """Function that raises an exception"""
    raise ValueError("test error")


class TestJob:
    """Tests for Job dataclass"""

    def test_job_creation(self) -> None:
        """Test creating a Job"""
        job = Job(func=simple_func, args=(5,), kwargs={})
        assert job.func is simple_func
        assert job.args == (5,)
        assert job.kwargs == {}

    def test_job_defaults(self) -> None:
        """Test Job default values"""
        job = Job(func=simple_func)
        assert job.args == ()
        assert job.kwargs == {}

    def test_job_is_frozen(self) -> None:
        """Test that Job is immutable (frozen)"""
        job = Job(func=simple_func)
        with pytest.raises(AttributeError):
            # noinspection PyDataclass
            job.func = lambda x: x


class TestRunConcurrent:
    """Tests for run_concurrent function"""

    def test_run_concurrent_basic(self) -> None:
        """Test basic concurrent execution"""
        jobs = [Job(func=simple_func, args=(i,)) for i in range(5)]
        results = run_concurrent(jobs)
        assert sorted(results) == [0, 2, 4, 6, 8]

    def test_run_concurrent_with_max_workers(self) -> None:
        """Test concurrent execution with max_workers limit"""
        jobs = [Job(func=simple_func, args=(i,)) for i in range(10)]
        results = run_concurrent(jobs, max_workers=2)
        assert len(results) == 10

    def test_run_concurrent_return_exceptions(self) -> None:
        """Test returning exceptions instead of raising"""
        jobs = [Job(func=failing_func)]
        results = run_concurrent(jobs, return_exceptions=True)
        assert len(results) == 1
        assert isinstance(results[0], ValueError)

    def test_run_concurrent_raises_by_default(self) -> None:
        """Test that exceptions are raised by default"""
        jobs = [Job(func=failing_func)]
        with pytest.raises(ValueError, match="test error"):
            run_concurrent(jobs)

    def test_run_concurrent_with_kwargs(self) -> None:
        """Test concurrent execution with keyword arguments"""

        def func_with_kwargs(a: int, b: int = 0) -> int:
            return a + b

        jobs = [Job(func=func_with_kwargs, args=(1,), kwargs={"b": 2})]
        results = run_concurrent(jobs)
        assert results == [3]

    @pytest.mark.parametrize("is_subprocess", [True, False])
    def test_run_concurrent_is_subprocess(self, mocker: MockFixture, is_subprocess: bool) -> None:
        """Test is_subprocess flag limits workers by CPU count"""
        cpu_count = multiprocessing.cpu_count()
        num_jobs = cpu_count * 3
        jobs = [Job(func=simple_func, args=(i,)) for i in range(num_jobs)]

        # Mock ThreadPoolExecutor to capture max_workers
        mock_executor = mocker.MagicMock()
        mock_thread_pool = mocker.patch("common_libs.job_executor.ThreadPoolExecutor")
        mock_thread_pool.return_value.__enter__.return_value = mock_executor

        # Mock executor.submit to return completed futures
        mock_futures = [mocker.MagicMock() for _ in jobs]
        for i, mock_future in enumerate(mock_futures):
            mock_future.result.return_value = i * 2  # Match simple_func output
        mock_executor.submit.side_effect = mock_futures

        # Mock futures.as_completed to return mocked futures
        mocker.patch("common_libs.job_executor.futures.as_completed", return_value=mock_futures)

        # Run with is_subprocess flag
        results = run_concurrent(jobs, is_subprocess=is_subprocess)

        # Verify ThreadPoolExecutor was called with max_workers limited to cpu_count when is_subprocess is True
        mock_thread_pool.assert_called_once_with(max_workers=cpu_count if is_subprocess else num_jobs)
        assert len(results) == num_jobs

    @pytest.mark.parametrize(
        ("num_jobs", "max_workers", "expected_workers"),
        [
            (5, 3, 3),  # max_workers < num_jobs: respects desired limit
            (2, 10, 2),  # max_workers > num_jobs: capped to number of jobs
            (5, None, 5),  # no max_workers: uses number of jobs
        ],
        ids=["max-workers-respected", "max-workers-capped-by-jobs", "max-workers-not-set"],
    )
    def test_run_concurrent_max_workers_behavior(
        self, mocker: MockFixture, num_jobs: int, max_workers: int | None, expected_workers: int
    ) -> None:
        """Test that max_workers is capped by job count and respects desired limit"""
        jobs = [Job(func=simple_func, args=(i,)) for i in range(num_jobs)]

        mock_executor = mocker.MagicMock()
        mock_thread_pool = mocker.patch("common_libs.job_executor.ThreadPoolExecutor")
        mock_thread_pool.return_value.__enter__.return_value = mock_executor

        mock_futures = [mocker.MagicMock() for _ in jobs]
        for i, mock_future in enumerate(mock_futures):
            mock_future.result.return_value = i * 2
        mock_executor.submit.side_effect = mock_futures
        mocker.patch("common_libs.job_executor.futures.as_completed", return_value=mock_futures)

        run_concurrent(jobs, max_workers=max_workers)

        mock_thread_pool.assert_called_once_with(max_workers=expected_workers)
