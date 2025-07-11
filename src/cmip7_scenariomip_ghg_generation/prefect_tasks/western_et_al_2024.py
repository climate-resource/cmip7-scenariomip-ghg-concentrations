"""
Western et al. (2024, https://doi.org/10.5281/zenodo.10782689) processing tasks
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import prefect.futures
from prefect import task

from cmip7_scenariomip_ghg_generation.notebook_running import run_notebook
from cmip7_scenariomip_ghg_generation.prefect_helpers import PathHashesCP, submit_output_aware, task_standard_path_cache
from cmip7_scenariomip_ghg_generation.prefect_tasks import download_file, extract_zip


@task(
    task_run_name="clean-western-et-al-2024-data_{file_to_clean}",
    persist_result=True,
    cache_policy=PathHashesCP(
        parameters_ignore=("file_to_clean",),
        parameters_output=("out_file",),
    ),
)
def clean_western_et_al_2024_data(
    root_extraction_dir: Path,
    file_to_clean: Path,
    out_file: Path,
) -> Path:
    """
    Clean the Western et al., 2024 data from its raw format

    Parameters
    ----------
    root_extraction_dir
        Root directory in which the Western et al. (2024) data was unpacked

    file_to_clean
        File, relative to `root_extraction_dir`, to clean

    out_file
        Path in which to save the extracted data

    Returns
    -------
    :
        Path to the cleaned data
    """
    raw = pd.read_csv(root_extraction_dir / file_to_clean, skiprows=1)

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


@task_standard_path_cache(
    task_run_name="extend-western-et-al-2024_{ghg}",
    parameters_output=("out_file",),
)
def extend_western_et_al_2024(  # noqa: PLR0913
    ghg: str,
    western_et_al_2024_clean: Path,
    out_file: Path,
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

    out_file
        Path in which to write the outpu

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


def get_western_et_al_2024_clean(
    western_et_al_2024_download_url: str,
    western_et_al_2024_raw_tar_file: Path,
    western_et_al_2024_extract_path: Path,
    western_et_al_2024_extracted_file_of_interest: Path,
    out_file: Path,
) -> prefect.futures.PrefectFuture[Path]:
    """
    Get clean Western et al. (2024) data

    Parameters
    ----------
    western_et_al_2024_download_url
        URL from which to download the raw data

    western_et_al_2024_raw_tar_file
        File in which to save the raw tar file

    western_et_al_2024_extract_path
        Path in which to extract the data

    western_et_al_2024_extracted_file_of_interest
        File of interest from the extracted data

    out_file
        File in which to save the outputs

    Returns
    -------
    :
        Future for the output file
    """
    tar_file_downloaded = submit_output_aware(
        download_file,
        url=western_et_al_2024_download_url,
        out_path=western_et_al_2024_raw_tar_file,
    )

    extracted = submit_output_aware(
        extract_zip,
        zip_file=tar_file_downloaded,
        extract_root_dir=western_et_al_2024_extract_path,
    )
    res = submit_output_aware(
        clean_western_et_al_2024_data,
        root_extraction_dir=extracted,
        file_to_clean=western_et_al_2024_extracted_file_of_interest,
        out_file=out_file,
    )

    return res
