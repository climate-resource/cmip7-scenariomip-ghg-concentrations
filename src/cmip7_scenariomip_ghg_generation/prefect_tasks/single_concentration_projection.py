"""
Single concentration projection tasks
"""

from __future__ import annotations

from pathlib import Path

from cmip7_scenariomip_ghg_generation.notebook_running import run_notebook
from cmip7_scenariomip_ghg_generation.prefect_helpers import task_path_cache


@task_path_cache(
    task_run_name="create-single-concentration-projection-annual-mean-file_{ghg}",
    # refresh_cache=True,
)
def create_single_concentration_projection_annual_mean_file(  # noqa: PLR0913
    ghg: str,
    cleaned_data_path: Path,
    historical_data_root_dir: Path,
    out_file: Path,
    raw_notebooks_root_dir: Path,
    executed_notebooks_dir: Path,
) -> Path:
    """
    Create annual-mean file for a GHG that has a single concentration projection

    Parameters
    ----------
    ghg
        GHG for which to create the annual-mean

    cleaned_data_path
        Path in which the cleaned data has been saved

    historical_data_root_dir
        Root path in which the historical data was downloaded

    out_file
        Path in which to write the annual-mean file

    raw_notebooks_root_dir
        Directory in which the raw notebooks live

    executed_notebooks_dir
        Directory in which executed notebooks should be written

    Returns
    -------
    :
        Written path
    """
    run_notebook(
        raw_notebooks_root_dir / "0010_create-single-concentration-projection-annual-mean-file.py",
        parameters={
            "ghg": ghg,
            "cleaned_data_path": str(cleaned_data_path),
            "historical_data_root_dir": str(historical_data_root_dir),
            "out_file": str(out_file),
        },
        run_notebooks_dir=executed_notebooks_dir,
        identity=ghg,
    )

    return out_file
