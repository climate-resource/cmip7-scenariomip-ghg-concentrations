"""
Convert annual-means to monthly-means
"""

from __future__ import annotations

from pathlib import Path

from cmip7_scenariomip_ghg_generation.notebook_running import run_notebook
from cmip7_scenariomip_ghg_generation.prefect_helpers import task_standard_path_cache


@task_standard_path_cache(
    task_run_name="interpolate-annual-mean-to-monthly_{ghg}",
    parameters_output=("out_file",),
    # refresh_cache=True,
)
def interpolate_annual_mean_to_monthly(  # noqa: PLR0913
    ghg: str,
    annual_mean_file: Path,
    historical_data_root_dir: Path,
    historical_data_seasonality_lat_gradient_info_root: Path,
    wmo_2022_clean_file: Path | None,
    out_file: Path,
    raw_notebooks_root_dir: Path,
    executed_notebooks_dir: Path,
) -> Path:
    """
    Interpolate annual-mean data to monthly

    Parameters
    ----------
    ghg
        GHG for which to create the annual-mean

    annual_mean_file
        Path in which the annual-mean data is written

    historical_data_root_dir
        Root path in which the historical data was downloaded

    historical_data_seasonality_lat_gradient_info_root
        Root path in which the seasonality and lat. gradient info was extracted

    wmo_2022_clean_file
        Path to the clean WMO 2022 data

        Required to handle Halon-1202, which isn't in the historical CMIP7 data for some reason.

    out_file
        File in which to write the monthly-mean file

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
        raw_notebooks_root_dir / "1000_interpolate-annual-mean-to-monthly.py",
        parameters={
            "ghg": ghg,
            "annual_mean_file": str(annual_mean_file),
            "historical_data_root_dir": str(historical_data_root_dir),
            "historical_data_seasonality_lat_gradient_info_root": str(
                historical_data_seasonality_lat_gradient_info_root
            ),
            "wmo_2022_clean_file": str(wmo_2022_clean_file) if wmo_2022_clean_file else "not_used",
            "out_file": str(out_file),
        },
        run_notebooks_dir=executed_notebooks_dir,
        identity=out_file.stem,
    )

    return out_file
