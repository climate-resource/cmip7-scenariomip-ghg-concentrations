"""
Split input emissions into individual files
"""

from __future__ import annotations

from pathlib import Path

import tqdm.auto
from pandas_openscm.io import load_timeseries_csv

from cmip7_scenariomip_ghg_generation.prefect_helpers import (
    task_standard_path_cache,
)
from cmip7_scenariomip_ghg_generation.scenario_info import ScenarioInfo


@task_standard_path_cache(
    task_run_name="split-input-emissions-into-scenarios_{emissions_file.name}",
    parameters_output=("out_dir",),
    refresh_cache=True,
)
def split_input_emissions_into_individual_files(
    emissions_file: Path, scenario_infos: tuple[ScenarioInfo, ...], out_dir: Path
) -> dict[ScenarioInfo | str, Path]:
    """
    Split the input emissions into invidiual files

    Parameters
    ----------
    emissions_file
        Input emissions file

    scenario_infos
        Info about the scenarios we want to use in this run

    out_dir
        Directory in which to write the split files

    Returns
    -------
    :
        Mapping from scenario information (or just "historical" for the history)
        to the path the emissions are written in
    """
    all_emissions = load_timeseries_csv(
        emissions_file,
        index_columns=["model", "scenario", "region", "variable", "unit"],
        out_columns_type=int,
        out_columns_name="year",
    )

    history_loc = all_emissions.index.get_level_values("scenario") == "historical"
    history = all_emissions.loc[history_loc]
    scenarios = all_emissions.loc[~history_loc]

    out_dir.mkdir(exist_ok=True, parents=True)

    history_out_file = out_dir / "historical.feather"
    history.to_feather(history_out_file)
    res = {"historical": history_out_file}

    scenarios_d = {
        (model, scenario): msdf
        for (model, scenario), msdf in tqdm.auto.tqdm(
            scenarios.groupby(["model", "scenario"]), desc="Splitting scenarios into individual DataFrame's"
        )
    }
    for si in tqdm.auto.tqdm(scenario_infos, desc="Saving scenarios into individual scenarios"):
        out_file = out_dir / f"{si.to_file_stem()}.feather"
        msdf = scenarios_d[(si.model, si.scenario)].dropna(how="all", axis="columns")
        msdf.to_feather(out_file)
        res[si] = out_file

    return res
