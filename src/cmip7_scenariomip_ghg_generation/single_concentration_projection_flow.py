"""
Tasks for gases which only have a single concentration projection
"""

import itertools
from pathlib import Path

from cmip7_scenariomip_ghg_generation.prefect_tasks import (
    calculate_inverse_emissions,
    create_esgf_files,
    create_single_concentration_projection_annual_mean_file,
    download_cmip7_historical_ghg_concentrations,
    interpolate_annual_mean_to_monthly,
    scale_lat_gradient_based_on_emissions,
    scale_seasonality_based_on_annual_mean,
)
from cmip7_scenariomip_ghg_generation.scenario_info import ScenarioInfo


def create_scenariomip_ghgs_single_concentration_projection(  # noqa: PLR0913
    ghgs: tuple[str, ...],
    scenario_infos: tuple[ScenarioInfo, ...],
    cleaned_data_path: Path,
    cmip7_historical_ghg_concentration_source_id: str,
    cmip7_historical_ghg_concentration_data_root_dir: Path,
    cmip7_historical_seasonality_lat_gradient_info_extracted: Path,
    annual_mean_dir: Path,
    monthly_mean_dir: Path,
    seasonality_dir: Path,
    inverse_emission_dir: Path,
    lat_gradient_dir: Path,
    esgf_ready_root_dir: Path,
    raw_notebooks_root_dir: Path,
    executed_notebooks_dir: Path,
) -> tuple[Path, ...]:
    """
    Create the ScenarioMIP GHG concentrations for GHGs based on WMO 2022

    Parameters
    ----------
    ghgs
        Greenhouse gases for which to create output files

    cleaned_data_path
        Path in which the cleaned data is saved

    cmip7_historical_ghg_concentration_source_id
        Source ID (unique identifier) for historical CMIP7 GHG concentrations

    cmip7_historical_ghg_concentration_data_root_dir
        Root directory for saving CMIP7 historical GHG concentrations

    cmip7_historical_seasonality_lat_gradient_info_extracted
        Root directory in which the historical lat. gradient and seasonality was extracted

    annual_mean_dir
        Path in which to save interim annual-mean data

    monthly_mean_dir
        Path in which to save interim monthly-mean data

    seasonality_dir
        Path in which to save interim seasonality data

    inverse_emission_dir
        Path in which to save inverse emissions data

    lat_gradient_dir
        Path in which to save interim latitudinal gradient data

    esgf_ready_root_dir
        Path to use as the root for writing ESGF-ready data

    raw_notebooks_root_dir
        Root directory for raw notebooks

    executed_notebooks_dir
        Path in which to write executed notebooks

    Returns
    -------
    :
        Generated paths
    """
    downloaded_cmip7_historical_ghgs_futures = {
        ghg: download_cmip7_historical_ghg_concentrations.submit(
            ghg,
            source_id=cmip7_historical_ghg_concentration_source_id,
            root_dir=cmip7_historical_ghg_concentration_data_root_dir,
        )
        for ghg in ghgs
    }

    global_mean_yearly_file_futures = {
        ghg: create_single_concentration_projection_annual_mean_file.submit(
            ghg=ghg,
            cleaned_data_path=cleaned_data_path,
            historical_data_root_dir=cmip7_historical_ghg_concentration_data_root_dir,
            annual_mean_dir=annual_mean_dir,
            raw_notebooks_root_dir=raw_notebooks_root_dir,
            executed_notebooks_dir=executed_notebooks_dir,
            wait_for=[cleaned_data_path, downloaded_cmip7_historical_ghgs_futures[ghg]],
        )
        for ghg in ghgs
    }

    global_mean_monthly_file_futures = {
        ghg: interpolate_annual_mean_to_monthly.submit(
            ghg=ghg,
            annual_mean_file=yearly_future,
            historical_data_root_dir=cmip7_historical_ghg_concentration_data_root_dir,
            historical_data_seasonality_lat_gradient_info_root=(
                cmip7_historical_seasonality_lat_gradient_info_extracted
            ),
            monthly_mean_dir=monthly_mean_dir,
            raw_notebooks_root_dir=raw_notebooks_root_dir,
            executed_notebooks_dir=executed_notebooks_dir,
        )
        for ghg, yearly_future in global_mean_yearly_file_futures.items()
    }

    seasonality_file_futures = {
        ghg: scale_seasonality_based_on_annual_mean.submit(
            ghg=ghg,
            annual_mean_file=yearly_future,
            historical_data_root_dir=cmip7_historical_ghg_concentration_data_root_dir,
            historical_data_seasonality_lat_gradient_info_root=(
                cmip7_historical_seasonality_lat_gradient_info_extracted
            ),
            seasonality_dir=seasonality_dir,
            raw_notebooks_root_dir=raw_notebooks_root_dir,
            executed_notebooks_dir=executed_notebooks_dir,
        )
        for ghg, yearly_future in global_mean_yearly_file_futures.items()
    }

    inverse_emissions_file_futures = {
        ghg: calculate_inverse_emissions.submit(
            ghg=ghg,
            monthly_mean_file=monthly_future,
            inverse_emission_dir=inverse_emission_dir,
            raw_notebooks_root_dir=raw_notebooks_root_dir,
            executed_notebooks_dir=executed_notebooks_dir,
        )
        for ghg, monthly_future in global_mean_monthly_file_futures.items()
    }

    lat_gradient_file_futures = {
        ghg: scale_lat_gradient_based_on_emissions.submit(
            ghg=ghg,
            annual_mean_emissions_file=inverse_emmissions_file,
            historical_data_root_dir=cmip7_historical_ghg_concentration_data_root_dir,
            historical_data_seasonality_lat_gradient_info_root=(
                cmip7_historical_seasonality_lat_gradient_info_extracted
            ),
            out_file=lat_gradient_dir / f"{ghg}_latitudinal-gradient-info.nc",
            raw_notebooks_root_dir=raw_notebooks_root_dir,
            executed_notebooks_dir=executed_notebooks_dir,
        )
        for ghg, inverse_emmissions_file in inverse_emissions_file_futures.items()
    }

    esgf_ready_futures = {
        ghg: create_esgf_files.submit(
            ghg=ghg,
            model=si.model,
            scenario=si.scenario,
            cmip_scenario_name=si.cmip_scenario_name,
            global_mean_monthly_file=global_mean_monthly_file_futures[ghg],
            seasonality_file=seasonality_file_futures[ghg],
            lat_gradient_file=lat_gradient_file_futures[ghg],
            esgf_ready_root_dir=esgf_ready_root_dir,
            raw_notebooks_root_dir=raw_notebooks_root_dir,
            executed_notebooks_dir=executed_notebooks_dir,
        )
        for ghg, si in itertools.product(global_mean_monthly_file_futures, scenario_infos)
    }

    esgf_written_paths = tuple(v for future in esgf_ready_futures.values() for v in future.result())
    return esgf_written_paths
