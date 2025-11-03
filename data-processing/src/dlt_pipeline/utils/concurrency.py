import time
from typing import Callable, Type, Tuple


def retry(
    func: Callable,
    exceptions: Tuple[Type[BaseException], ...],
    attempts: int,
    backoff_sec: float,
    on_error: Callable[[BaseException, int], None] = None,
):
    """Simple retry wrapper with constant backoff.

    Calls `func` with no args; for args/kwargs, use lambda: retry(lambda: f(x), ...)
    """
    for attempt in range(1, attempts + 1):
        try:
            return func()
        except exceptions as e:
            if on_error:
                on_error(e, attempt)
            if attempt == attempts:
                raise
            time.sleep(backoff_sec)


