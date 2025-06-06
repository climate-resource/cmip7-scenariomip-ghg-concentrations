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
    # refresh_cache=True,
)
def make_complete_scenario(  # noqa: PLR0913
    scenario_info: ScenarioInfo,
    scenario_file: Path,
    history_file: Path,
    out_file: Path,
    raw_notebooks_root_dir: Path,
    executed_notebooks_dir: Path,
) -> Path:
    run_notebook(
        raw_notebooks_root_dir / "0020_create-complete-emission-scenario.py",
        parameters={
            "model": scenario_info.model,
            "scenario": scenario_info.scenario,
            "scenario_file": str(scenario_file),
            "history_file": str(history_file),
            "out_file": str(out_file),
        },
        run_notebooks_dir=executed_notebooks_dir,
        identity=out_file.stem,
    )

    return out_file
