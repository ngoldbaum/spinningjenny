from __future__ import annotations
from time import sleep
from threading import Lock, RLock

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
def test_resource_usage(usecs: int, num_threads: int) -> None:
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
        result = executor.map_unordered(task, (factory.create() for _ in range(1000)))
        assert len(list(result)) == 1000
    # Give it a little leeway in case it goes over:
    assert factory.max < 10 * num_threads


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
    retrieved = 0
    with ThreadPoolExecutor(num_threads) as executor:
        result = executor.map_unordered(lambda _: tasks.run(), range(100), buffersize=20)
        while tasks.get_ran() < 20 + num_threads:
            sleep(0.001)
        assert tasks.get_ran() == 20 + num_threads
        sleep(0.01)
        assert tasks.get_ran() == 20 + num_threads
        result.next()
        result.next()
        result.next()
        while tasks.get_ran() < 20 + num_threads + 3:
            sleep(0.001)
        assert tasks.get_ran() == 20 + num_threads + 3
        sleep(0.01)
        assert tasks.get_ran() == 20 + num_threads + 3
        list(result)
