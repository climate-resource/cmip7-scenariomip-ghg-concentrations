"""
Plotting tasks
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

from prefect import task
from prefect.cache_policies import INPUTS

from cmip7_scenariomip_ghg_generation.notebook_running import run_notebook
from cmip7_scenariomip_ghg_generation.scenario_info import ScenarioInfo


@task(task_run_name="plot-marker-overview", persist_result=True, cache_policy=INPUTS)
def plot_marker_overview(  # noqa: PLR0913
    scenario_info_markers: tuple[ScenarioInfo, ...],
    emissions_complete_dir: Path,
    magicc_output_db_dir: Path,
    magicc_db_backend_str: str,
    dependency_complete_files: tuple[Path, ...],
    complete_file: Path,
    raw_notebooks_root_dir: Path,
    executed_notebooks_dir: Path,
) -> Path:
    """
    Plot the overview of the marker scenarios

    Parameters
    ----------
    scenario_info_markers
        Scenario information for the markers

    emissions_complete_dir
        Directory in which complete emissions are written

    magicc_output_db_dir
        Root directory of the MAGICC output database

    magicc_db_backend_str
        Name of the MAGICC database backend

    dependency_complete_files
        Complete files for upstream tasks

    complete_file
        Complete file for this task

    raw_notebooks_root_dir
        Root directory of the raw notebooks

    executed_notebooks_dir
        Directory in which to write executed notebooks

    Returns
    -------
    :
        Complete file
    """
    scenario_info_markers_str = ";;".join(
        ";".join((si.model, si.scenario, si.cmip_scenario_name)) for si in scenario_info_markers
    )
    run_notebook(
        raw_notebooks_root_dir / "2000_plot-marker-overview.py",
        parameters={
            "scenario_info_markers": scenario_info_markers_str,
            "emissions_complete_dir": str(emissions_complete_dir),
            "magicc_output_db_dir": str(magicc_output_db_dir),
            "magicc_db_backend_str": magicc_db_backend_str,
        },
        run_notebooks_dir=executed_notebooks_dir,
        identity="only",
    )

    with open(complete_file, "w") as fh:
        fh.write(str(dt.datetime.utcnow()))

    return complete_file
