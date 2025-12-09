"""
A collection of helpers for working with [prefect](https://docs.prefect.io/v3/get-started)
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable, Coroutine, Iterable
from dataclasses import dataclass
from enum import Enum
from functools import partial
from pathlib import Path
from typing import Any, ParamSpec, TypeVar

from gcages.hashing import get_file_hash
from prefect import Task, task
from prefect.cache_policies import INPUTS, TASK_SOURCE, CachePolicy
from prefect.context import TaskRunContext
from prefect.futures import PrefectFuture
from prefect.locking.filesystem import FileSystemLockManager
from prefect.settings import PREFECT_HOME
from prefect.states import State
from prefect.transactions import IsolationLevel

R = TypeVar("R")
P = ParamSpec("P")

task_basic_cache = partial(
    task,
    persist_result=True,
    cache_policy=(INPUTS + TASK_SOURCE).configure(
        # Ensure that only one task can run for this cache key at a time.
        # (Don't get two tasks doing the same work)
        # See https://docs-3.prefect.io/v3/develop/task-caching#cache-isolation
        isolation_level=IsolationLevel.SERIALIZABLE,
        lock_manager=FileSystemLockManager(lock_files_directory=PREFECT_HOME.value() / "locks"),
    ),
    # Uncomment the line below to force a refresh of the cache
    # (can also use the PREFECT_TASKS_REFRESH_CACHE env variable to do the same)
    # refresh_cache=True,
)
"""
A convenience to help us avoid redefining the same caching policy over and over

Note that this cache does not handle missing output paths well.
For that, use [submit_output_aware][].
"""


class FileHashCachingResult(Enum):
    """
    Possible states when getting a file's hash for caching
    """

    DOES_NOT_EXIST = 1
    IS_FILE = 2
    IS_DIR = 3


def get_file_hash_for_cache(value: Path) -> tuple[FileHashCachingResult, str | None]:
    """
    Get file hash for cache

    See comments for explanation.
    TODO: clean up once we're happy with this pattern.
    """
    if not value.exists():
        # Return key that will cause a cache miss
        # (turns out that using None doesn't work
        # if you're combining multiple caching strategies
        # because it is just dropped).
        cache_key = str(dt.datetime.utcnow().timestamp())

        # Note for devs:
        # This pattern might not be quite right,
        # because things can end up being run twice
        # if the value doesn't exist at cache key calculation time.
        # (Although, as far as I can tell,
        # the cache key is computed after other tasks have finished
        # so this might not actually be a problem.)
        # However, in that case there is no way
        # to produce a stable cache key
        # so this might be the best we can do.
        # Maybe using transactions or clever waits would fix it,
        # but I can't figure that out how right now.
        # Transaction docs for reference:
        # https://docs-3.prefect.io/v3/develop/transactions#idempotency
        return (FileHashCachingResult.DOES_NOT_EXIST, cache_key)

    if value.is_file():
        return (FileHashCachingResult.IS_FILE, get_file_hash(value))

    return (FileHashCachingResult.IS_DIR, None)


@dataclass
class PathHashesCP(CachePolicy):
    """
    Policy for computing a cache key based on the path hashes in the callable's inputs

    If any path does not exist, then an invalid key is returned.
    If the path exists and is a file,
    then its hash is used to create the cache key.
    """

    parameters_ignore: tuple[str, ...] | None = None
    """
    Parameters to ignore when checking paths
    """

    parameters_output: tuple[str, ...] | None = None
    """
    Parameters to treat as output

    These aren't included in the generated cache key
    because they don't always exist at the time the cache key is evaluated.
    In other words, including them results in the following undesirable behaviour.

    1. The cache key is generated without the output file existing
    1. The task runs
    1. The output is generated
    1. On the next run, the cache key is generated with the output file's hash too
    1. Hence the key is different from the first run and the cache misses
    1. So the task runs again (unnecessarily)
    """

    def compute_key(  # noqa: PLR0912
        self,
        task_ctx: TaskRunContext,
        inputs: dict[str, Any] | None,
        flow_parameters: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> str | None:
        """
        Compute cache key

        Parameters
        ----------
        task_ctx
            Task context

        inputs
            Inputs to the callable

        flow_parameters
            Parameters used at the flow level

        **kwargs
            Other keyword arguments

        Returns
        -------
        :
            Hash if available otherwise `None` if the key cannot be calculated
        """
        if not task_ctx:
            return None

        hash_l = []

        for parameter in sorted(inputs.keys()):
            if self.parameters_ignore is not None and parameter in self.parameters_ignore:
                # Ignore the parameter
                continue

            if self.parameters_output is not None and parameter in self.parameters_output:
                # Don't include the value when calculating the hash key
                continue

            value = inputs[parameter]

            if isinstance(value, Path):
                path_values_to_check = (value,)

            elif not isinstance(value, str) and isinstance(value, Iterable):
                path_values_to_check = [v for v in value if isinstance(v, Path)]

            else:
                path_values_to_check = None

            if path_values_to_check:
                for path_value in path_values_to_check:
                    file_hash_result = get_file_hash_for_cache(path_value)
                    if file_hash_result[0] == FileHashCachingResult.DOES_NOT_EXIST:
                        # Return instantly as we should have a cache miss
                        return file_hash_result[1]

                    elif file_hash_result[0] == FileHashCachingResult.IS_DIR:
                        # Do nothing
                        pass

                    elif file_hash_result[0] == FileHashCachingResult.IS_FILE:
                        # Add hash of file values that haven't been ignored
                        hash_l.append(file_hash_result[1])

                    else:
                        raise NotImplementedError(file_hash_result)

        if not hash_l:
            # Only directories
            # Return static key as we only need to check for existence
            return "static-key"

        # All files exist, return concatenated hashes as the hash key
        key = "_".join(hash_l)

        return key


def task_standard_path_cache(
    parameters_ignore: tuple[str, ...] | None = None,
    parameters_output: tuple[str, ...] | None = None,
    **kwargs: Any,
) -> Task:
    """
    Get a task with standard path caching

    Parameters
    ----------
    parameters_ignore
        Parameters to ignore when checking if paths exist or have changed

    parameters_output
        Parameters to treat as output files

        These aren't included in the generated cache key,
        but the cache is invalid if these files don't exist
    **kwargs
        Passed to [prefect.task.Task][]

    Returns
    -------
    :
        Initialised task
    """
    return task(
        persist_result=True,
        cache_policy=INPUTS
        + TASK_SOURCE
        + PathHashesCP(
            parameters_ignore=parameters_ignore,
            parameters_output=parameters_output,
        ),
        **kwargs,
    )


def submit_output_aware(
    task: Task[P, R] | Task[P, Coroutine[Any, Any, R]],
    *args: P.args,
    **kwargs: P.kwargs,
) -> PrefectFuture[R] | State[R]:
    """
    Submit to a task, being aware of whether the output exists or not

    If the output does not exist,
    then the task is updated to force a refresh of the cache.

    This function relies on the task's cache policy
    including a [PathHashesCP][] instance.
    In other words, your task definition should look something like

    ```python
    from prefect import task

    @task(
        ...
        cache_policy=... + PathHashesCP(
            parameters_output=("out_path",)
        ),
        ...
    )
    def my_task(url: Path, para_a: float, out_path: Path) -> Path:
        ...
    ````

    Parameters
    ----------
    task
        Task to submit to

    *args
        Arguments to pass to the task

    **kwargs
        Keyword arguments to pass to the task

    Returns
    -------
    :
        Result of calling [task.submit][] with `args` and `kwargs`

    Notes
    -----
    This behaviour isn't native to prefect.
    I can sort of see why, it relies on re-running a task just based
    on whether the output exists or not.
    In contrast, prefect's normal logic is:
    if a task has been run with the same inputs and same source,
    it should give the same output
    and outputs should not disappear because of behaviour outside the workflows.

    This is fair enough, but to get more 'Make'-like behaviour
    (where re-execution is triggered simply by deleting a file),
    I wanted something like this.
    Despite the relatively simple solution, this took a surprising amount of thinking.
    Maybe one day we split this out into its own package
    or make a PR back into prefect with the relevant pieces.
    """
    if isinstance(task.cache_policy, PathHashesCP):
        phcp = task.cache_policy

    elif hasattr(task.cache_policy, "policies"):
        for policy in task.cache_policy.policies:
            if isinstance(policy, PathHashesCP):
                phcp = policy
                break
        else:
            msg = f"No instance of `PathHashesCP` in the tasks's cache policies. {task.cache_policy=}"
            raise AssertionError(msg)
    else:
        msg = (
            "task's cache policy is not and does not contain "
            "an instance of `PathHashesCP` in the tasks's cache policies. "
            f"{task.cache_policy=}"
        )
        raise AssertionError(msg)

    parameters_output = phcp.parameters_output
    task_updated = task
    if parameters_output is not None:
        for parameter in parameters_output:
            if parameter not in kwargs:
                msg = (
                    f"{parameter}. "
                    "For this to work, the output parameters must be kwargs. "
                    f"{task.name=} {parameter=} {kwargs=} {args=}"
                )
                raise KeyError(msg)

            if not kwargs[parameter].exists():
                # Output doesn't exist, force cache to refresh
                task_updated = task_updated.with_options(refresh_cache=True)
                break

    return task_updated.submit(*args, **kwargs)


def create_hash_dict(
    files: Iterable[Path],
    hash_func: Callable[[Path], str] = get_file_hash,
    exclusions: Iterable[Callable[[Path], bool]] | None = None,
) -> dict[Path, str]:
    """
    Create dictionary of hashes for files

    Parameters
    ----------
    files
        Files to create hashes for

    hash_func
        Function to use to get the hash of files

    exclusions
        An iterable of callables. These are applied to each file. If any of the
        results is `True` then the file is skipped and will not be included
        in the dictionary of calculated hashes.

    Returns
    -------
    :
        Dictionary of hashes for each file which was not excluded. The keys
        are the file paths (as they appear in `files`) and the values are
        the calculated hashes.
    """
    out = {}
    for fp in files:
        if exclusions is not None and any(excl(fp) for excl in exclusions):
            # Don't include this file
            continue

        out[fp] = get_file_hash(fp)

    return out


def write_hash_dict_to_file(
    hash_dict: dict[Path, str],
    checklist_file: Path,
    relative_to: Path | None = None,
) -> Path:
    """
    Write a dictionary of hashes to a file

    Parameters
    ----------
    hash_dict
        Dictionary of paths and their hashes

    checklist_file
        Where to write the checklist file.

    relative_to
        If supplied, the file paths are written relative to this path

        In other words, the full path will not be written in the checklist file

    Returns
    -------
    :
        Path of the generated checklist file
    """
    with open(checklist_file, "w") as fh:
        # sort to ensure same result for same set of files
        for fp in sorted(hash_dict.keys()):
            if relative_to is None:
                fp_write = fp
            else:
                fp_write = fp.relative_to(relative_to)

            fh.write(f"{hash_dict[fp]}  {fp_write}\n")

    return checklist_file
