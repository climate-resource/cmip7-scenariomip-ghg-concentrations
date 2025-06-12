"""
Make complete scenarios

This means adding on emissions that aren't provided by the IAMs
but are needed to run MAGICC.
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
    task_run_name="make-complete-scenario_{scenario_info.model}_{scenario_info.scenario}",
    persist_result=True,
    cache_policy=(INPUTS - "pool" - "res_timeout")
    # + TASK_SOURCE
    + PathHashesCP(
        parameters_output=("out_file",),
    ),
)
def make_complete_scenario(  # noqa: PLR0913
    scenario_info: ScenarioInfo,
    scenario_file: Path,
    inverse_emissions_file: Path,
    history_file: Path,
    out_file: Path,
    raw_notebooks_root_dir: Path,
    executed_notebooks_dir: Path,
    pool: multiprocessing.pool.Pool | None,
    res_timeout: int = 10 * 60,
) -> Path:
    """
    Make a complete scenario from the relevant input pieces

    Parameters
    ----------
    scenario_info
        Scenario info

    scenario_file
        File containing the raw scenario data from the IAM

    inverse_emissions_file
        File containing inverse emissions based on GHGs that use a single concentration projection

    history_file
        File containing a full set of historical emissions

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
        notebook=raw_notebooks_root_dir / "0020_create-complete-emission-scenario.py",
        # verbose=True,
        # progress=True,
        parameters={
            "model": scenario_info.model,
            "scenario": scenario_info.scenario,
            "scenario_file": str(scenario_file),
            "inverse_emissions_file": str(inverse_emissions_file),
            "history_file": str(history_file),
            "out_file": str(out_file),
        },
        run_notebooks_dir=executed_notebooks_dir,
        identity=out_file.stem,
        logger=get_run_logger(),
        kwargs_to_show_in_logging=("identity",),
        timeout=res_timeout,
    )

    return out_file
