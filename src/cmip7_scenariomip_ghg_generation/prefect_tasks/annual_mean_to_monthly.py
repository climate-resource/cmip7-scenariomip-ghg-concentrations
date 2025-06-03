"""
Convert annual-means to monthly-means
"""

from __future__ import annotations

from pathlib import Path

from cmip7_scenariomip_ghg_generation.notebook_running import run_notebook
from cmip7_scenariomip_ghg_generation.prefect_helpers import task_standard_cache


@task_standard_cache(
    task_run_name="interpolate-annual-mean-to-monthly_{ghg}",
    # refresh_cache=True,
)
def interpolate_annual_mean_to_monthly(  # noqa: PLR0913
    ghg: str,
    annual_mean_file: Path,
    historical_data_root_dir: Path,
    historical_data_seasonality_lat_gradient_info_root: Path,
    monthly_mean_dir: Path,
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

    monthly_mean_dir
        Directory in which to write the monthly-mean file

    raw_notebooks_root_dir
        Directory in which the raw notebooks live

    executed_notebooks_dir
        Directory in which executed notebooks should be written

    Returns
    -------
    :
        Written path
    """
    if "annual" not in annual_mean_file.name:
        raise AssertionError(annual_mean_file.name)

    if not annual_mean_file.name.endswith(".feather"):
        raise AssertionError(annual_mean_file.name)

    out_file = monthly_mean_dir / annual_mean_file.name.replace("annual", "monthly").replace(".feather", ".nc")

    run_notebook(
        raw_notebooks_root_dir / "1000_interpolate-annual-mean-to-monthly.py",
        parameters={
            "ghg": ghg,
            "annual_mean_file": str(annual_mean_file),
            "historical_data_root_dir": str(historical_data_root_dir),
            "historical_data_seasonality_lat_gradient_info_root": str(
                historical_data_seasonality_lat_gradient_info_root
            ),
            "out_file": str(out_file),
        },
        run_notebooks_dir=executed_notebooks_dir,
        identity=out_file.stem,
    )

    return out_file
