"""
WMO 2022 processing tasks
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from cmip7_scenariomip_ghg_generation.prefect_helpers import task_standard_path_cache


@task_standard_path_cache(
    task_run_name="clean-wmo-data_{raw_data_path}",
    parameters_output=("out_file",),
)
def clean_wmo_data(raw_data_path: Path, out_file: Path) -> Path:
    """
    Clean the WMO data from its raw format

    Parameters
    ----------
    raw_data_path
        Path to the raw data

    out_file
        Path in which to save the extracted data

    Returns
    -------
    :
        Path to the cleaned data
    """
    raw = pd.read_excel(raw_data_path)

    column_renaming = {}
    for c in raw.columns:
        if c == "Year":
            column_renaming[c] = "year"

        column_renaming[c] = c.lower().replace("-", "")

    # Hard-coded, you just have to know this
    raw["unit"] = "ppt"

    res = (
        raw.rename(column_renaming, axis="columns")
        .set_index(["year", "unit"])
        .melt(ignore_index=False, var_name="ghg")
        .set_index("ghg", append=True)["value"]
        .unstack("year")
    )

    out_file.parent.mkdir(exist_ok=True, parents=True)
    res.to_feather(out_file)

    return out_file
