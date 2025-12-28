from __future__ import annotations

import logging
import time
from typing import Callable, Iterable, Tuple, Type, TypeVar


logger = logging.getLogger(__name__)

T = TypeVar("T")


def retry_call(
    func: Callable[[], T],
    *,
    retries: int = 3,
    backoff_s: float = 0.5,
    retry_exceptions: Tuple[Type[BaseException], ...] = (Exception,),
    logger_name: str | None = None,
) -> T:
    attempt = 0
    last_exc: BaseException | None = None

    while attempt <= retries:
        try:
            return func()
        except retry_exceptions as exc:
            last_exc = exc
            if attempt >= retries:
                break
            wait = backoff_s * (2 ** attempt)
            (logging.getLogger(logger_name) if logger_name else logger).warning(
                "Retrying after error (attempt %d/%d, wait=%.2fs): %s",
                attempt + 1,
                retries + 1,
                wait,
                exc,
            )
            time.sleep(wait)
            attempt += 1

    assert last_exc is not None
    raise last_exc
