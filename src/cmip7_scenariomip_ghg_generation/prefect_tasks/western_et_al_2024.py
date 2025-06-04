"""
Western et al. (2024, https://doi.org/10.5281/zenodo.10782689) processing tasks
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from cmip7_scenariomip_ghg_generation.notebook_running import run_notebook
from cmip7_scenariomip_ghg_generation.prefect_helpers import task_path_cache


@task_path_cache(task_run_name="clean-western-et-al-2024-data_{raw_data_path}")
def clean_western_et_al_2024_data(raw_data_path: Path, out_file: Path) -> Path:
    """
    Clean the Western et al., 2024 data from its raw format

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
    raw = pd.read_csv(raw_data_path, skiprows=1)

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


@task_path_cache(task_run_name="extend-western-et-al-2024_{ghg}")
def extend_western_et_al_2024(
    ghg: str,
    western_et_al_2024_clean: Path,
    wmo_2022_clean: Path,
    raw_notebooks_root_dir: Path,
    executed_notebooks_dir: Path,
) -> Path:
    """
    Extend Western et al. (2024) data

    Parameters
    ----------
    ghg
        GHG we're working on

    western_et_al_2024_clean
        Path to clean Western et al. (2024) data

    wmo_2022_clean
        Path to clean WMO (2022) data

        Used for comparing to how WMO (2022) did their extensions

    raw_notebooks_root_dir
        Directory in which to copy raw notebooks

    executed_notebooks_dir
        Directory in which to write executed notebooks


    Returns
    -------
    :
        Path to the file with extended data
    """
    out_file = western_et_al_2024_clean.parent / f"western-et-al-2024_{ghg}_extended.feather"
    run_notebook(
        raw_notebooks_root_dir / "0001_extend-western-et-al-2024.py",
        parameters={
            "ghg": ghg,
            "western_et_al_2024_clean_path": str(western_et_al_2024_clean),
            "wmo_2022_clean_path": str(wmo_2022_clean),
            "out_file": str(out_file),
        },
        run_notebooks_dir=executed_notebooks_dir,
        identity="only",
    )

    return out_file
