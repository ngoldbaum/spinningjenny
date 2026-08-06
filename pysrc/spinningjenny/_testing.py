from time import time_ns


def run_for_usecs(microsecs: int, *_args) -> None:
    """Run for given number of microseconds."""
    del _args
    if microsecs == 0:
        return
    start_ns = time_ns()
    nanos = microsecs * 1000
    while time_ns() - start_ns < nanos:
        pass
