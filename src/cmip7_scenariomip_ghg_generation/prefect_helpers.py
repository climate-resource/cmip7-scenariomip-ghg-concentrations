"""
A collection of helpers for working with [prefect](https://docs.prefect.io/v3/get-started)
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any

from gcages.hashing import get_file_hash
from prefect import task
from prefect.cache_policies import INPUTS, TASK_SOURCE, CachePolicy
from prefect.context import TaskRunContext

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


@dataclass
class PathHashes(CachePolicy):
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

    def compute_key(
        self,
        task_ctx: TaskRunContext,
        inputs: dict[str, Any] | None,
        flow_parameters: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> str | None:
        """
        Compute cache key

        Note: this caching strategy causes things to be run at least twice
        because the output file hashes can't be predicted in advance
        (therefore things have to run once, then run again to create the cache key
        but this hash key isn't in the database so the function runs once more
        before the cache key can actually work,
        there is probably a way around this but I haven't found it yet).

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
            Hash if available otherwise `None` if the cache is clearly invalid
        """
        if not task_ctx:
            return None

        hash_l = []

        for parameter in sorted(inputs.keys()):
            if self.parameters_ignore is not None and parameter in self.parameters_ignore:
                # Ignore the parameter
                continue

            value = inputs[parameter]

            if isinstance(value, Path):
                if not value.exists():
                    # Return key that will be invalid soon
                    # (turns out that using None doesn't work
                    # if you're combining multiple caching strategies)
                    return str(dt.datetime.utcnow().timestamp())

                if value.is_file():
                    hash_l.append(get_file_hash(value))

        if not hash_l:
            # Only directories
            # Return static key as we only check for existence
            return "static-key"

        # All files exist, return concatenated hashes as the hash key
        return "_".join(hash_l)


task_path_cache = partial(
    task,
    persist_result=True,
    cache_policy=INPUTS + TASK_SOURCE + PathHashes(),
    # Uncomment the line below to force a refresh of the cache
    # (can also use the PREFECT_TASKS_REFRESH_CACHE env variable to do the same)
    # refresh_cache=True,
)
"""
A convenience to help us avoid redefining the same caching policy over and over
"""


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
