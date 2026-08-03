import sys
from time import time_ns
from os import cpu_count
from concurrent.futures import ThreadPoolExecutor

from spinningjenny import ThreadPoolExecutor as SpinExecutor


def spin_100us(_x):
    # Spin for 100µs, hopefully exactly:
    start_ns = time_ns()
    while time_ns() - start_ns < 100_000:
        pass


def stdlib_executor(n):
    with ThreadPoolExecutor(cpu_count()) as pool:
        result = pool.map(spin_100us, range(n))
        assert len(list(result)) == n
    return cpu_count()


def spin_executor(n):
    with SpinExecutor(cpu_count()) as pool:
        result = pool.map(spin_100us, range(n))
        assert len(list(result)) == n
    return cpu_count()


def go(n, jobs=(
        "stdlib_executor",
        "spin_executor",
    )):
    for f_name in jobs:
        print(f_name)
        f = globals()[f_name]
        start = time_ns()
        for _ in range(50):
            num_cores = f(n)
        # We could each thread's time in the total; if we got the same clock
        # time with twice as many threads, the overhead is twice as high:
        total = (num_cores * (time_ns() - start) / (1000 * 50))
        # Remove time spent spinning in the actual job:
        total -= n * 100
        print("per-call µs", total / n)
        print()


if __name__ == "__main__":
    length = 100
    if len(sys.argv) >= 2:
        length = int(sys.argv[1])
    if len(sys.argv) == 3:
        jobs = [sys.argv[2]]
        go(length, jobs)
    else:
        go(length)
