"""
Simple climate model (SCM) running
"""

from __future__ import annotations

import multiprocessing
from pathlib import Path

from prefect import task
from prefect.cache_policies import INPUTS
from prefect.logging import get_run_logger

from cmip7_scenariomip_ghg_generation.notebook_running import run_notebook
from cmip7_scenariomip_ghg_generation.parallelisation import call_maybe_in_subprocess
from cmip7_scenariomip_ghg_generation.prefect_helpers import (
    PathHashesCP,
)
from cmip7_scenariomip_ghg_generation.scenario_info import ScenarioInfo


@task(
    task_run_name="run-magicc_{magicc_version}_{scenario_info.model}_{scenario_info.scenario}",
    persist_result=True,
    cache_policy=(INPUTS - "n_magicc_workers" - "pool" - "res_timeout")
    # + TASK_SOURCE
    + PathHashesCP(
        parameters_ignore=None,
        parameters_output=("out_file",),
    ),
)
def run_magicc(  # noqa: PLR0913
    scenario_info: ScenarioInfo,
    complete_file: Path,
    magicc_version: str,
    magicc_exe: Path,
    magicc_prob_distribution: Path,
    n_magicc_workers: int,
    out_file: Path,
    raw_notebooks_root_dir: Path,
    executed_notebooks_dir: Path,
    pool: multiprocessing.pool.Pool | None,
    res_timeout: int = 10 * 60,
) -> Path:
    """
    Run MAGICC

    Parameters
    ----------
    scenario_info
        Scenario info

    complete_file
        File containing the complete scenario data including historical

    magicc_version
        MAGICC version to run

    magicc_exe
        Path to the MAGICC executable to use

    magicc_prob_distribution
        Path to the MAGICC probabilistic distribution to use

    n_magicc_workers
        Number of MAGICC workers to use when running

    out_file
        File in which to write the output

    raw_notebooks_root_dir
        Directory in which the raw notebooks live

    executed_notebooks_dir
        Directory in which to write the executed notebooks

    pool
        Parallel processing pool to use for running

        If `None`, no parallel processing is used

    res_timeout
        Time to wait for parallel results before timing out

    Returns
    -------
    :
        Written file
    """
    call_maybe_in_subprocess(
        run_notebook,
        maybe_pool=pool,
        notebook=raw_notebooks_root_dir / "0021_run-magicc.py",
        # verbose=True,
        progress=True,
        parameters={
            "model": scenario_info.model,
            "scenario": scenario_info.scenario,
            "complete_file": str(complete_file),
            "magicc_version": magicc_version,
            "magicc_exe": str(magicc_exe),
            "magicc_prob_distribution": str(magicc_prob_distribution),
            "n_magicc_workers": n_magicc_workers,
            "out_file": str(out_file),
        },
        run_notebooks_dir=executed_notebooks_dir,
        identity=out_file.stem,
        logger=get_run_logger(),
        kwargs_to_show_in_logging=("identity",),
        timeout=res_timeout,
    )

    return out_file
