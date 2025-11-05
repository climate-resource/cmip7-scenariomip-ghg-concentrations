"""
Create gradient-aware harmonisation based annual-mean file
"""

from __future__ import annotations

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
    task_run_name="create-gradient-aware-harmonisation-annual-mean-file_{ghg}",
    persist_result=True,
    cache_policy=(INPUTS - "pool" - "res_timeout")
    # + TASK_SOURCE
    + PathHashesCP(
        parameters_ignore=None,
        parameters_output=("out_file",),
    ),
)
def create_gradient_aware_harmonisation_annual_mean_file(  # noqa: PLR0913
    ghg: str,
    scenario_info_markers: tuple[ScenarioInfo, ...],
    historical_data_root_dir: Path,
    magicc_output_db_dir: Path,
    magicc_db_backend_str: str,
    out_file: Path,
    raw_notebooks_root_dir: Path,
    executed_notebooks_dir: Path,
    pool: multiprocessing.pool.Pool | None,
    res_timeout: int = 10 * 60,
) -> Path:
    """
    Create an annual mean file based on gradient-aware harmonisation

    Parameters
    ----------
    ghg
        Greenhouse gas to create the annual-mean file for

    scenario_info_markers
        Scenario info about the marker scenarios

    historical_data_root_dir
        Root dir of the historical GHG concentration data

    magicc_output_db_dir
        Root directory of the MAGICC output database

    magicc_db_backend_str
        Name of the MAGICC database backend

    out_file
        File in which to save the output

    raw_notebooks_root_dir
        Root directory in which raw notebooks live

    executed_notebooks_dir
        Directory in which to save executed notebooks

    pool
        Multiprocessing pool to use for running the notebooks

        If `None`, notebooks are not run in separate processes

    res_timeout
        Timeout to wait when trying to retrieve notebook running results

    Returns
    -------
    :
        `out_file`
    """
    scenario_info_markers_str = ";;".join(
        ";".join((si.model, si.scenario, si.cmip_scenario_name)) for si in scenario_info_markers
    )
    call_maybe_in_subprocess(
        run_notebook,
        maybe_pool=pool,
        notebook=raw_notebooks_root_dir / "0031_create-projection-gradient-aware-harmonisation.py",
        # verbose=2,
        # progress=True,
        parameters={
            "ghg": ghg,
            "scenario_info_markers": scenario_info_markers_str,
            "historical_data_root_dir": str(historical_data_root_dir),
            "magicc_output_db_dir": str(magicc_output_db_dir),
            "magicc_db_backend_str": magicc_db_backend_str,
            "out_file": str(out_file),
        },
        run_notebooks_dir=executed_notebooks_dir,
        identity=ghg,
        logger=get_run_logger(),
        kwargs_to_show_in_logging=("identity", "notebook"),
        timeout=res_timeout,
    )

    return out_file
