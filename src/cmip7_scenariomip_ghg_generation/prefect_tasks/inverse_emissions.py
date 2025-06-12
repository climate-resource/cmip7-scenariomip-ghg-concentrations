"""
Calculate inverse emissions
"""

from __future__ import annotations

import multiprocessing
from pathlib import Path

import pandas as pd
import pandas_indexing as pix
from prefect import task
from prefect.cache_policies import INPUTS
from prefect.logging import get_run_logger

from cmip7_scenariomip_ghg_generation.notebook_running import run_notebook
from cmip7_scenariomip_ghg_generation.parallelisation import call_maybe_in_subprocess
from cmip7_scenariomip_ghg_generation.prefect_helpers import (
    PathHashesCP,
    task_standard_path_cache,
)


@task(
    task_run_name="calculate-inverse-emissions_{ghg}",
    persist_result=True,
    cache_policy=(INPUTS - "pool" - "res_timeout")
    # + TASK_SOURCE
    + PathHashesCP(
        parameters_output=("out_file",),
    ),
)
def calculate_inverse_emissions(  # noqa: PLR0913
    ghg: str,
    monthly_mean_file: Path,
    out_file: Path,
    raw_notebooks_root_dir: Path,
    executed_notebooks_dir: Path,
    pool: multiprocessing.pool.Pool | None,
    res_timeout: int = 10 * 60,
) -> Path:
    """
    Calculate inverse emissions

    Parameters
    ----------
    ghg
        GHG for which to create the annual-mean

    monthly_mean_file
        Path in which the monthly-mean data is written

    out_file
        File in which to write the inverse emissions

    raw_notebooks_root_dir
        Directory in which the raw notebooks live

    executed_notebooks_dir
        Directory in which executed notebooks should be written

    pool
        Parallel processing pool to use for running

        If `None`, no parallel processing is used

    res_timeout
        Time to wait for parallel results before timing out

    Returns
    -------
    :
        Written path
    """
    call_maybe_in_subprocess(
        run_notebook,
        maybe_pool=pool,
        notebook=raw_notebooks_root_dir / "1020_calculate-inverse-emissions.py",
        # verbose=True,
        progress=True,
        parameters={
            "ghg": ghg,
            "monthly_mean_file": str(monthly_mean_file),
            "out_file": str(out_file),
        },
        run_notebooks_dir=executed_notebooks_dir,
        identity=out_file.stem,
        logger=get_run_logger(),
        kwargs_to_show_in_logging=("identity",),
        timeout=res_timeout,
    )

    return out_file


@task_standard_path_cache(
    task_run_name="compile-inverse-emissions",
    parameters_output=("out_file",),
    # refresh_cache=True,
)
def compile_inverse_emissions(in_files: tuple[Path, ...], out_file: Path) -> Path:
    """
    Compile inverse emissions into a single file

    This also only keeps the emissions with emissions units, not ppt units.

    Parameters
    ----------
    in_files
        Input files to compile from

    out_file
        File in which to write the compiled inverse emissions

    Returns
    -------
    :
        Written path
    """
    raw = pix.concat(pd.read_feather(f) for f in in_files)

    emms_units = raw.loc[~raw.index.get_level_values("unit").str.startswith("pp")]
    if emms_units.shape[0] != len(in_files):
        msg = "Different number of timeseries to what was expected"
        raise AssertionError(msg)

    emms_units.to_feather(out_file)

    return out_file
