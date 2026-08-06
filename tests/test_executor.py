from __future__ import annotations
from threading import RLock

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
