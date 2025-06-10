"""
Scale future latitudinal gradient with emissions
"""

from __future__ import annotations

from pathlib import Path

from cmip7_scenariomip_ghg_generation.notebook_running import run_notebook
from cmip7_scenariomip_ghg_generation.prefect_helpers import task_standard_path_cache


@task_standard_path_cache(
    task_run_name="scale-lat-gradient-based-on-emissions_{ghg}_{annual_mean_emissions_file.stem}",
    parameters_output=("out_file",),
)
def scale_lat_gradient_based_on_emissions(  # noqa: PLR0913
    ghg: str,
    annual_mean_emissions_file: Path,
    historical_data_root_dir: Path,
    historical_data_seasonality_lat_gradient_info_root: Path,
    out_file: Path,
    raw_notebooks_root_dir: Path,
    executed_notebooks_dir: Path,
) -> Path:
    """
    Scale latitudinal gradient based on annual-mean emissions

    Parameters
    ----------
    ghg
        GHG for which to create the latitudinal gradient

    annual_mean_emissions_file
        Path in which the annual-mean emissions data is written

    historical_data_root_dir
        Root path in which the historical data was downloaded

    historical_data_seasonality_lat_gradient_info_root
        Root path in which the seasonality and lat. gradient info was extracted

    out_file
        Output file

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
        raw_notebooks_root_dir / "1030_scale-latitudinal-gradient-based-on-emissions.py",
        parameters={
            "ghg": ghg,
            "annual_mean_emissions_file": str(annual_mean_emissions_file),
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


@task_standard_path_cache(
    task_run_name="scale-lat-gradient-eofs_{ghg}_{annual_mean_emissions_file.stem}",
    parameters_output=("out_file",),
)
def scale_lat_gradient_eofs(  # noqa: PLR0913
    ghg: str,
    annual_mean_emissions_file: Path,
    historical_data_root_dir: Path,
    historical_data_seasonality_lat_gradient_info_root: Path,
    out_file: Path,
    raw_notebooks_root_dir: Path,
    executed_notebooks_dir: Path,
) -> Path:
    """
    Scale latitudinal gradient EOFs

    Parameters
    ----------
    ghg
        GHG for which to create the latitudinal gradient

    annual_mean_emissions_file
        Path in which the annual-mean emissions data is written

    historical_data_root_dir
        Root path in which the historical data was downloaded

    historical_data_seasonality_lat_gradient_info_root
        Root path in which the seasonality and lat. gradient info was extracted

    out_file
        Output file

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
        raw_notebooks_root_dir / "1031_scale-latitudinal-gradient-eofs.py",
        parameters={
            "ghg": ghg,
            "annual_mean_emissions_file": str(annual_mean_emissions_file),
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
