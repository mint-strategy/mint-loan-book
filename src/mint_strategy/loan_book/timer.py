import logging
import time
import typing

import humanize

log = logging.getLogger(__name__)


class Timer:
    def __init__(self, name: str = "Task"):
        self._name = name
        self._bytes_processed = None

    def __enter__(self):
        log.info("Starting %s", self._name)
        self._start = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = time.perf_counter() - self._start

        if self._bytes_processed is None:
            log.info("%s: %.2fs", self._name, elapsed)
        else:
            speed = self._bytes_processed / elapsed
            log.info("%s: %s in %.2fs; speed: %s/s",
                     self._name,
                     humanize.naturalsize(self._bytes_processed),
                     elapsed,
                     humanize.naturalsize(speed))

    def _set_bytes_processed(self, bytes_processed: int) -> None:
        self._bytes_processed = bytes_processed

    bytes_processed = property(fset=_set_bytes_processed)

    def with_func(self, func: typing.Callable, *args, **vargs) -> typing.Any:
        with self:
            return func(*args, **vargs)
