from __future__ import annotations
from time import sleep
from threading import Lock, RLock, Condition

import pytest

from spinningjenny import ThreadPoolExecutor
from spinningjenny._testing import run_for_usecs


def test_minimal():
    with ThreadPoolExecutor(2) as executor:
        assert sorted(executor.map_unordered(lambda x: x * 2, range(3))) == [0, 2, 4]


class Resource:
    """
    A resource that can created concurrently, a stand-in for memory.
    """

    def __init__(self, factory: ResourceFactory):
        self.factory = factory

    def __del__(self):
        self.factory._destroy()


class ResourceFactory:
    """Factory for ``Resource`` instances."""

    def __init__(self):
        self.lock = RLock()
        self.max = 0
        self.resources = 0

    def create(self) -> Resource:
        with self.lock:
            self.resources += 1
            self.max = max(self.max, self.resources)
        return Resource(self)

    def _destroy(self) -> None:
        with self.lock:
            self.resources -= 1


@pytest.mark.parametrize("usecs", [0, 10, 100])
@pytest.mark.parametrize("num_threads", [2, 4, 6])
@pytest.mark.parametrize("buffersize", [None, 10, 100])
def test_resource_usage(usecs: int, num_threads: int, buffersize: None | int) -> None:
    """
    The amounts of resources used by the executor should be constained.

    Resources would include memory (if _instantiating_ a task uses a lot of
    memory), or other places where instantiating many tasks in parallel is
    expensive.
    """
    factory = ResourceFactory()

    def task(resource):
        assert isinstance(resource, Resource)
        run_for_usecs(usecs)

    with ThreadPoolExecutor(num_threads) as executor:
        result = executor.map_unordered(
            task, (factory.create() for _ in range(1000)), buffersize=buffersize
        )
        assert len(list(result)) == 1000
    # Give it some leeway in case it goes over:
    assert factory.max < 30 * num_threads


class TasksRun:
    """Track how many tasks ran."""

    def __init__(self):
        self.lock = Lock()
        self.ran = 0

    def run(self):
        with self.lock:
            self.ran += 1

    def get_ran(self):
        with self.lock:
            return self.ran


@pytest.mark.parametrize("num_threads", [2, 4, 6])
def test_buffersize_limits_execution_when_no_iteration(num_threads: int) -> None:
    """
    If ``buffersize`` is set, at most ``buffersize + num_threads`` tasks can be
    executed before work stops so long as no iteration is happening.
    """
    tasks = TasksRun()
    with ThreadPoolExecutor(num_threads) as executor:
        result = executor.map_unordered(
            lambda _: tasks.run(), range(100), buffersize=20
        )
        while not result._is_full():
            pass
        ran = tasks.get_ran()
        assert 20 <= ran <= 20 + num_threads
        # If we're full, sleeping should only be able to add tasks in the race
        # condition between hitting full and the rest of the threads finishing
        # a task and blocking on sending to the full queue:
        sleep(0.01)
        assert 20 <= tasks.get_ran() <= 20 + num_threads
        next(result)
        next(result)
        next(result)
        while not result._is_full():
            pass
        assert 23 <= tasks.get_ran() <= ran + num_threads + 3
        # Get the rest, ensure everything ran:
        list(result)
        assert tasks.get_ran() == 100


@pytest.mark.parametrize("buffersize", [None, 5])
def test_drop_without_iterating_over_all_items(buffersize: None | int) -> None:
    """
    Dropping the results iterator doesn't stop execution.
    """
    counter = []
    results = Condition()

    def inc(x):
        run_for_usecs(10)
        with results:
            counter.append(x)
            if len(counter) == 1000:
                results.notify()
        return x

    with ThreadPoolExecutor(2) as executor:
        iterator = executor.map_unordered(inc, range(1000), buffersize=buffersize)
        next(iterator)
        del iterator

    with results:
        results.wait(10)
    assert sorted(counter) == list(range(1000))


def test_drop_does_not_panic() -> None:
    """Dropping the results iterator doesn't panic."""
    executor = ThreadPoolExecutor(2)
    it = executor.map_unordered(lambda x: x, range(1000), buffersize=5)
    next(it)
    del it
