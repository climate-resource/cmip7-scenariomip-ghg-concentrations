"""
Simple climate model (SCM) running
"""

from __future__ import annotations

import datetime as dt
import multiprocessing
from pathlib import Path

from prefect import task
from prefect.cache_policies import INPUTS
from prefect.logging import get_run_logger

from cmip7_scenariomip_ghg_generation.notebook_running import run_notebook
from cmip7_scenariomip_ghg_generation.parallelisation import call_maybe_in_subprocess
from cmip7_scenariomip_ghg_generation.prefect_helpers import PathHashesCP
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
    db_dir: Path,
    db_backend_str: str,
    out_file: Path,
    raw_notebooks_root_dir: Path,
    executed_notebooks_dir: Path,
    pool: multiprocessing.pool.Pool | None,
    res_timeout: int = 100 * 60,
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

    db_dir
        Root directory of the database in which to save the outputs

    db_backend_str
        String name of the database backend

    out_file
        Path in which to write a timestamp of the time at which this job was complete

        Used to help with getting the dependencies between tasks
        and caching right.

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
    # # TODO: remove this and fix caching so we don't get so many accidental hits
    # if out_file.exists():
    #     return out_file
    call_maybe_in_subprocess(
        run_notebook,
        maybe_pool=pool,
        notebook=raw_notebooks_root_dir / "0021_run-magicc.py",
        verbose=2,
        progress=True,
        parameters={
            "model": scenario_info.model,
            "scenario": scenario_info.scenario,
            "complete_file": str(complete_file),
            "magicc_version": magicc_version,
            "magicc_exe": str(magicc_exe),
            "magicc_prob_distribution": str(magicc_prob_distribution),
            "n_magicc_workers": n_magicc_workers,
            "db_dir": str(db_dir),
            "db_backend_str": db_backend_str,
        },
        run_notebooks_dir=executed_notebooks_dir,
        identity=f"{magicc_version}_{scenario_info.model}_{scenario_info.scenario}",
        logger=get_run_logger(),
        kwargs_to_show_in_logging=("identity", "notebook"),
        timeout=res_timeout,
    )

    with open(out_file, "w") as fh:
        fh.write(str(dt.datetime.utcnow()))

    return out_file
