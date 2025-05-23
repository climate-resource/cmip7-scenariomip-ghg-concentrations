"""
WMO 2022 processing tasks
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from cmip7_scenariomip_ghg_generation.prefect_helpers import task_standard_cache


@task_standard_cache(task_run_name="extract-wmo-data_{raw_data_path}")
def extract_wmo_data(raw_data_path: Path, out_file: Path) -> Path:
    """
    Extract the WMO data from its raw format

    Parameters
    ----------
    raw_data_path
        Path to the raw data

    out_file
        Path in which to save the extracted data

    Returns
    -------
    :
        Path to the extracted data
    """
    raw = pd.read_excel(raw_data_path)

    column_renaming = {}
    for c in raw.columns:
        if c == "Year":
            column_renaming[c] = "year"

        column_renaming[c] = c.lower().replace("-", "")

    res = (
        raw.rename(column_renaming, axis="columns")
        .set_index("year")
        .melt(ignore_index=False, var_name="ghg")
        .set_index("ghg", append=True)["value"]
        .unstack("year")
    )
    # TODO: use pandas-openscm once we have merged
    # https://github.com/openscm/pandas-openscm/pull/18

    # Hard-coded, you just have to know this
    res["unit"] = "ppt"
    res = res.set_index("unit", append=True)

    out_file.parent.mkdir(exist_ok=True, parents=True)
    res.to_feather(out_file)

    return out_file


@task_standard_cache(task_run_name="get-wmo-ghgs_{extracted_data_path}")
def get_wmo_ghgs(extracted_data_path: Path) -> tuple[str, ...]:
    """
    Get the GHGs that should be based on WMO data

    Parameters
    ----------
    extracted_data_path
        Path to the extracted data

    Returns
    -------
    :
        GHGs to use from WMO data
    """
    ghgs = tuple(
        sorted(
            pd.read_feather(extracted_data_path).index.get_level_values("ghg").unique()
        )
    )

    return ghgs
