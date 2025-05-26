"""
WMO 2022 processing tasks
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from cmip7_scenariomip_ghg_generation.notebook_running import run_notebook
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
    Get the GHGs included in WMO data

    Parameters
    ----------
    extracted_data_path
        Path to the extracted data

    Returns
    -------
    :
        GHGs to use from WMO data
    """
    ghgs = tuple(sorted(pd.read_feather(extracted_data_path).index.get_level_values("ghg").unique()))

    return ghgs


@task_standard_cache(task_run_name="create-wmo-based-annual-mean-file_{ghg}", refresh_cache=True)
def create_wmo_based_annual_mean_file(  # noqa: PLR0913
    ghg: str,
    harmonise: bool,
    extracted_wmo_data_path: Path,
    historical_data_root_dir: Path,
    annual_mean_dir: Path,
    raw_notebooks_root_dir: Path,
    executed_notebooks_dir: Path,
) -> Path:
    """
    Create a WMO-based annual-mean file

    Parameters
    ----------
    ghg
        GHG for which to create the annual-mean

    harmonise
        Should we harmonise the WMO 2022 data to the historical data?

        If `False`, we check that the data is already harmonised.

    extracted_wmo_data_path
        Path in which the WMO data has been extracted

    historical_data_root_dir
        Root path in which the historical data was downloaded

    annual_mean_dir
        Directory in which to write the annual-mean file

    raw_notebooks_root_dir
        Directory in which the raw notebooks live

    executed_notebooks_dir
        Directory in which executed notebooks should be written

    Returns
    -------
    :
        Written path
    """
    out_file = annual_mean_dir / f"wmo-based_{ghg}_annual-mean.feather"

    run_notebook(
        raw_notebooks_root_dir / "0001_create-wmo-based-annual-mean-file.py",
        parameters={
            "ghg": ghg,
            "harmonise": harmonise,
            "extracted_wmo_data_path": str(extracted_wmo_data_path),
            "historical_data_root_dir": str(historical_data_root_dir),
            "out_file": str(out_file),
        },
        run_notebooks_dir=executed_notebooks_dir,
        identity=ghg,
    )

    return out_file
