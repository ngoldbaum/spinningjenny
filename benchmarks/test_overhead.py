from time import time_ns
from concurrent.futures import ThreadPoolExecutor as OrigExecutor

import pytest

from spinningjenny import ThreadPoolExecutor as SpinExecutor


def spin_100us(_x):
    # Spin for 100µs, hopefully exactly:
    start_ns = time_ns()
    while time_ns() - start_ns < 100_000:
        pass


def noop(_x):
    pass


class OrigExecutor(OrigExecutor):
    pass


@pytest.mark.parametrize("function", [noop, spin_100us])
@pytest.mark.parametrize("executor_factory", [OrigExecutor, SpinExecutor])
def test_one_thousand_calls(benchmark, function, executor_factory):
    def run():
        with executor_factory(8) as executor:
            result = executor.map(function, range(1000))
            return list(result)

    result = benchmark(run)
    assert len(result) == 1000
