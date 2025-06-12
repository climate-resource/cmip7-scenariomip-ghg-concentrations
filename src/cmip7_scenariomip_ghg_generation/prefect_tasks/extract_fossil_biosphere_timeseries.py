"""
Extract specific scenarios from a collection of timeseries
"""

from __future__ import annotations

from functools import partial
from pathlib import Path

import numpy as np
import pandas as pd
import pandas_indexing as pix
from pandas_openscm.indexing import multi_index_lookup
from pandas_openscm.io import load_timeseries_csv

from cmip7_scenariomip_ghg_generation.prefect_helpers import (
    task_standard_path_cache,
)
from cmip7_scenariomip_ghg_generation.scenario_info import ScenarioInfo


@task_standard_path_cache(
    task_run_name="extract-fossil-biosphere-timeseries",
    parameters_output=("out_file",),
)
def extract_fossil_biosphere_timeseries(
    extract_from: tuple[Path, ...],
    scenario_infos: tuple[ScenarioInfo, ...],
    out_file: Path,
) -> Path:
    """
    Extract complete fossil and biosphere timeseries for the given scenarios

    Parameters
    ----------
    extract_from
        Files from which to extract the timeseries

    scenario_infos
        Information about scenarios to extract

    out_file
        File in which to write the compiled data

    Returns
    -------
    :
        `out_file`
    """
    # Could make this injectable
    model_level = "model"
    scenario_level = "scenario"
    file_reader = partial(
        load_timeseries_csv,
        index_columns=[model_level, scenario_level, "variable", "unit"],
        out_columns_type=int,
        out_columns_name="year",
    )

    db = pix.concat([file_reader(f) for f in extract_from])

    mod_scen_index = pd.MultiIndex.from_tuples(
        [(si.model, si.scenario) for si in scenario_infos], names=[model_level, scenario_level]
    )

    mod_scens = multi_index_lookup(db, mod_scen_index).dropna(axis="columns")

    mod_scens_missing_years = np.setdiff1d(
        np.arange(mod_scens.columns.min(), mod_scens.columns.max() + 1), mod_scens.columns
    )
    mod_scens_lin_interp = mod_scens.copy()
    mod_scens_lin_interp.loc[:, mod_scens_missing_years] = np.nan
    mod_scens_lin_interp = mod_scens_lin_interp.sort_index(axis="columns").T.interpolate(method="index").T

    history = db.loc[pix.isin(scenario="historical")].dropna(axis="columns")
    history_aligned = history.pix.project(["variable", "unit"]).align(mod_scens)[0].dropna(axis="columns")

    not_in_history_years = mod_scens_lin_interp.columns.difference(history_aligned.columns)
    mod_scens_keep = mod_scens_lin_interp.loc[:, not_in_history_years]

    out = pix.concat([history_aligned, mod_scens_keep], axis="columns")

    out_file.parent.mkdir(exist_ok=True, parents=True)
    out.to_feather(out_file)

    return out_file
