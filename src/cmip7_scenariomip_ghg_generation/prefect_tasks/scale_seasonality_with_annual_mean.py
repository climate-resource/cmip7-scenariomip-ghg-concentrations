"""
Scale future seasonality with anual-mean
"""

from __future__ import annotations

from pathlib import Path

from cmip7_scenariomip_ghg_generation.notebook_running import run_notebook
from cmip7_scenariomip_ghg_generation.prefect_helpers import task_standard_path_cache


@task_standard_path_cache(
    task_run_name="scale-seasonality-based-on-annual-mean_{ghg}",
    parameters_output=("out_file",),
    # refresh_cache=True,
)
def scale_seasonality_based_on_annual_mean(  # noqa: PLR0913
    ghg: str,
    annual_mean_file: Path,
    historical_data_root_dir: Path,
    historical_data_seasonality_lat_gradient_info_root: Path,
    out_file: Path,
    raw_notebooks_root_dir: Path,
    executed_notebooks_dir: Path,
) -> Path:
    """
    Scale seasonality based on annual-mean concentrations

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

    out_file
        File in which to write the seasonality

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
        raw_notebooks_root_dir / "1010_scale-seasonality-based-on-annual-mean.py",
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
