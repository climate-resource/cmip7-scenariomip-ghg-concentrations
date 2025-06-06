"""
Make complete scenarios

This means adding on emissions that aren't provided by the IAMs
but are needed to run MAGICC.
"""

from __future__ import annotations

from pathlib import Path

from cmip7_scenariomip_ghg_generation.notebook_running import run_notebook
from cmip7_scenariomip_ghg_generation.prefect_helpers import task_standard_path_cache
from cmip7_scenariomip_ghg_generation.scenario_info import ScenarioInfo


@task_standard_path_cache(
    task_run_name="make-complete-scenario_{scenario_info.model}_{scenario_info.scenario}",
    parameters_output=("out_file",),
)
def make_complete_scenario(  # noqa: PLR0913
    scenario_info: ScenarioInfo,
    scenario_file: Path,
    inverse_emissions_file: Path,
    history_file: Path,
    out_file: Path,
    raw_notebooks_root_dir: Path,
    executed_notebooks_dir: Path,
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

    Returns
    -------
    :
        Written file
    """
    run_notebook(
        raw_notebooks_root_dir / "0020_create-complete-emission-scenario.py",
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
    )

    return out_file
