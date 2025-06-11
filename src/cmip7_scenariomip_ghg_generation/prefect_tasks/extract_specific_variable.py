"""
Extract a specific variable from a collection of timeseries
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pandas_indexing as pix

from cmip7_scenariomip_ghg_generation.prefect_helpers import (
    task_standard_path_cache,
)
from cmip7_scenariomip_ghg_generation.scenario_info import ScenarioInfo


@task_standard_path_cache(
    task_run_name="extract-specific-variable-from-collection_{variable_lower}",
    parameters_output=("out_file",),
)
def extract_specific_variable_from_collection(
    extract_from: tuple[Path, ...],
    scenario_infos: tuple[ScenarioInfo, ...],
    variable_lower: str,
    out_file: Path,
) -> Path:
    """
    Extract a specific variable from a collection of timeseries

    This also switches to CMIP scenario names

    Parameters
    ----------
    extract_from
        Files from which to extract the timeseries

    scenario_infos
        Information about scenarios

        Used to rename to CMIP scenario names

    variable_lower
        Lowercase version of the variable to extract

    out_file
        File in which to write the compiled data

    Returns
    -------
    :
        `out_file`
    """
    # Could make this injectable
    file_reader = pd.read_feather
    variable_level = "variable"
    model_level = "model"
    scenario_level = "scenario"
    unit_level = "unit"

    db = pix.concat([file_reader(f) for f in extract_from])

    raw = db.loc[db.index.get_level_values(variable_level).str.lower() == variable_lower]

    scenario_map = {(si.model, si.scenario): si.cmip_scenario_name for si in scenario_infos}
    cmip_scenario_names = raw.pix.project([model_level, scenario_level]).index.map(scenario_map)
    if cmip_scenario_names.isnull().any():
        raise AssertionError

    out = raw.pix.assign(scenario=cmip_scenario_names).pix.project([unit_level, scenario_level, variable_level])

    out_file.parent.mkdir(exist_ok=True, parents=True)
    out.to_feather(out_file)

    return out_file
