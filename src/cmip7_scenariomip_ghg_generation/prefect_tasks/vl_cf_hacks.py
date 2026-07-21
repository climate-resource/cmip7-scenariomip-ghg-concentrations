"""
Hacks to support vl-cf generation
"""

from __future__ import annotations

from pathlib import Path

from cmip7_scenariomip_ghg_generation.notebook_running import run_notebook
from cmip7_scenariomip_ghg_generation.prefect_helpers import task_standard_path_cache


@task_standard_path_cache(
    task_run_name="make-vl-cf-inputs-that-match-historical-and-vl_{ghg}",
    parameters_output=("out_file_global_mean_monthly", "out_file_seasonality", "out_file_lat_gradient"),
    # Hmph, more caching pain
    # refresh_cache=True,
)
def make_vl_cf_inputs_that_match_historical_and_vl(  # noqa: PLR0913
    ghg: str,
    internal_processing_scenario_name: str,
    esgf_files_start_year: int,
    monthly_mean_dir: Path,
    seasonality_dir: Path,
    lat_gradient_dir: Path,
    historical_data_seasonality_lat_gradient_info_root: Path,
    wmo_2022_clean_file: Path | None,
    out_file_global_mean_monthly: Path,
    out_file_seasonality: Path,
    out_file_lat_gradient: Path,
    raw_notebooks_root_dir: Path,
    executed_notebooks_dir: Path,
) -> tuple[Path, Path, Path]:
    """
    Make `vl-cf` inputs that are a combination of historical and vl

    Parameters
    ----------
    ghg
        GHG for which to create the inputs

    internal_processing_scenario_name
        The scenario name to use when writing out the files

    esgf_files_start_year
        Year in which ESGF files should start

    monthly_mean_dir
        Directory in which intermediate monthly-mean files are written

    seasonality_dir
        Directory in which intermediate seasonality files are written

    lat_gradient_dir
        Directory in which intermediate latitudinal gradient files are written

    historical_data_seasonality_lat_gradient_info_root
        Root path in which the seasonality and lat. gradient info was extracted

    wmo_2022_clean_file
        Path to the clean WMO 2022 data

        Required to handle Halon-1202, which isn't in the historical CMIP7 data for some reason.

    out_file_global_mean_monthly
        Path in which to write the new global-mean monthly file

    out_file_seasonality
        Path in which to write the new seasonality file

    out_file_lat_gradient
        Path in which to write the new latitudinal gradient file

    raw_notebooks_root_dir
        Directory in which the raw notebooks live

    executed_notebooks_dir
        Directory in which executed notebooks should be written

    Returns
    -------
    :
        Written paths (global-mean first, then seasonality then latitudinal gradient)
    """
    run_notebook(
        raw_notebooks_root_dir / "1050_hack-vl-cf-inputs.py",
        parameters={
            "ghg": ghg,
            "internal_processing_scenario_name": internal_processing_scenario_name,
            "esgf_files_start_year": esgf_files_start_year,
            "monthly_mean_dir": str(monthly_mean_dir),
            "seasonality_dir": str(seasonality_dir),
            "lat_gradient_dir": str(lat_gradient_dir),
            "historical_data_seasonality_lat_gradient_info_root": str(
                historical_data_seasonality_lat_gradient_info_root
            ),
            "wmo_2022_clean_file": str(wmo_2022_clean_file) if wmo_2022_clean_file else "not_used",
            "out_file_global_mean_monthly": str(out_file_global_mean_monthly),
            "out_file_seasonality": str(out_file_seasonality),
            "out_file_lat_gradient": str(out_file_lat_gradient),
        },
        run_notebooks_dir=executed_notebooks_dir,
        identity=f"1050_hack-vl-cf-inputs_{ghg}",
    )
    return (out_file_global_mean_monthly, out_file_seasonality, out_file_lat_gradient)
