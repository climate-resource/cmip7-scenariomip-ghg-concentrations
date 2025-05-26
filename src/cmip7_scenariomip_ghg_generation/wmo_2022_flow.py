"""
Tasks for gases which come from WMO 2022
"""

from pathlib import Path

from cmip7_scenariomip_ghg_generation.prefect_tasks import (
    create_wmo_based_annual_mean_file,
    download_cmip7_historical_ghg_concentrations,
    extract_wmo_data,
    get_wmo_ghgs,
    interpolate_annual_mean_to_monthly,
)


def create_scenariomip_ghgs_wmo_2022_based(  # noqa: PLR0913
    ghgs: tuple[str, ...],
    harmonise: bool,
    cmip7_historical_ghg_concentration_source_id: str,
    cmip7_historical_ghg_concentration_data_root_dir: Path,
    cmip7_historical_seasonality_lat_gradient_info_extracted: Path,
    wmo_raw_data_path: Path,
    wmo_extracted_data_path: Path,
    annual_mean_dir: Path,
    monthly_mean_dir: Path,
    raw_notebooks_root_dir: Path,
    executed_notebooks_dir: Path,
) -> tuple[Path, ...]:
    """
    Create the ScenarioMIP GHG concentrations for GHGs based on WMO 2022

    Parameters
    ----------
    ghgs
        Greenhouse gases for which to create output files

    harmonise
        Whether to harmonise the WMO 2022

        If `False`, we just check that the data is already harmonised
        to the historical values.

    cmip7_historical_ghg_concentration_source_id
        Source ID (unique identifier) for historical CMIP7 GHG concentrations

    cmip7_historical_ghg_concentration_data_root_dir
        Root directory for saving CMIP7 historical GHG concentrations

    cmip7_historical_seasonality_lat_gradient_info_extracted
        Root directory in which the historical lat. gradient and seasonality was extracted

    wmo_raw_data_path
        Path to raw WMO data

    wmo_extracted_data_path
        Path in which to extract the WMO data

    annual_mean_dir
        Path in which to save interim annual-mean data

    monthly_mean_dir
        Path in which to save interim monthly-mean data

    raw_notebooks_root_dir
        Root directory for raw notebooks

    executed_notebooks_dir
        Path in which to write executed notebooks

    Returns
    -------
    :
        Generated paths
    """
    extracted_wmo_data_path = extract_wmo_data(raw_data_path=wmo_raw_data_path, out_file=wmo_extracted_data_path)
    all_wmo_ghgs = get_wmo_ghgs(extracted_wmo_data_path)
    not_supported = set(ghgs) - set(all_wmo_ghgs)
    if not_supported:
        msg = f"These gases are not supplied by WMO 2022: {not_supported}"
        raise AssertionError(msg)

    downloaded_cmip7_historical_ghgs_futures = {
        ghg: download_cmip7_historical_ghg_concentrations.submit(
            ghg,
            source_id=cmip7_historical_ghg_concentration_source_id,
            root_dir=cmip7_historical_ghg_concentration_data_root_dir,
        )
        for ghg in ghgs
    }

    wmo_based_global_mean_yearly_file_futures = {
        ghg: create_wmo_based_annual_mean_file.submit(
            ghg=ghg,
            harmonise=harmonise,
            extracted_wmo_data_path=extracted_wmo_data_path,
            historical_data_root_dir=cmip7_historical_ghg_concentration_data_root_dir,
            annual_mean_dir=annual_mean_dir,
            raw_notebooks_root_dir=raw_notebooks_root_dir,
            executed_notebooks_dir=executed_notebooks_dir,
            wait_for=downloaded_cmip7_historical_ghgs_futures[ghg],
        )
        for ghg in ghgs
    }

    wmo_based_global_mean_monthly_file_futures = {
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
        for ghg, yearly_future in wmo_based_global_mean_yearly_file_futures.items()
    }

    # Trigger execution
    tuple(v.result() for v in wmo_based_global_mean_monthly_file_futures.values())

    # TODO: return written paths
