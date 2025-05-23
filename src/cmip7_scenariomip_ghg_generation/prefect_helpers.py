"""
A collection of helpers for working with [prefect](https://docs.prefect.io/v3/get-started)
"""

from __future__ import annotations

from functools import partial

from prefect import task
from prefect.cache_policies import INPUTS, TASK_SOURCE

task_standard_cache = partial(
    task,
    persist_result=True,
    cache_policy=INPUTS + TASK_SOURCE,
    # Uncomment the line below to force a refresh of the cache
    # (can also use the PREFECT_TASKS_REFRESH_CACHE env variable to do the same)
    # refresh_cache=True,
)
"""
A convenience to help us avoid redefining the same caching policy over and over
"""
