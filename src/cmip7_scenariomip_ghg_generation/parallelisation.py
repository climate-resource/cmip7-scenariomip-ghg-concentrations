"""
Parallelisation helpers
"""

from __future__ import annotations

import multiprocessing
import os
import threading
from collections.abc import Callable, Collection
from typing import ParamSpec, TypeVar

T = TypeVar("T")
P = ParamSpec("P")


def call_maybe_in_subprocess(
    func: Callable[P, T],
    maybe_pool: multiprocessing.pool.Pool | None,
    *args: P.args,
    logger: bool | None = None,
    timeout: int | None = None,
    kwargs_to_show_in_logging: Collection[str] | None = None,
    **kwargs: P.kwargs,
) -> T:
    """
    Call a function, maybe in a sub-process

    Parameters
    ----------
    func
        Function to call

    maybe_pool
        Argument which might be a [multiprocessing.pool.Pool][]

    logger
        If supplied, used for logging messages

    kwargs_to_show_in_logging
        Keyword arguments to show in logging messages

        This helps identify how the function is being called
        without just blindly printing all the args and kwargs.
        (Obviously this is very limited functionality
        and could be made much more flexible).

    timeout
        Timeout to wait for job results to be returned

    *args
        Passed to `func`

    **kwargs
        Passed to `func`

    Returns
    -------
    :
        Results of calling `func(*args, **kwargs)`
    """
    if logger:
        if kwargs_to_show_in_logging:
            paras_logging = {k: kwargs[k] for k in kwargs_to_show_in_logging}
            logging_info_text = f"{func.__name__} with {paras_logging}"

        else:
            logging_info_text = func.__name__

    if not maybe_pool:
        if logger:
            logger.debug(f"Running {logging_info_text} without a parallel pool")
        return func(*args, **kwargs)

    if logger:
        logger.info(f"Submitting {logging_info_text} to the parallel pool in {os.getpid()=} {threading.get_ident()=}")
    res_async = maybe_pool.apply_async(func, args, kwargs)

    if logger:
        logger.info(f"Waiting for the results of {logging_info_text} in {os.getpid()=} {threading.get_ident()=}")
    res = res_async.get(timeout=timeout)

    if logger:
        logger.info(f"Received the results of {logging_info_text} in {os.getpid()=} {threading.get_ident()=}")

    return res
