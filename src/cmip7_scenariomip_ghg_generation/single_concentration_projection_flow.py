"""
Tasks for gases which only have a single concentration projection
"""

import itertools
import multiprocessing.pool
from pathlib import Path

import prefect.futures
from attrs import define

from cmip7_scenariomip_ghg_generation.prefect_helpers import submit_output_aware
from cmip7_scenariomip_ghg_generation.prefect_tasks import (
    calculate_inverse_emissions,
    create_esgf_files,
    create_single_concentration_projection_annual_mean_file,
    download_cmip7_historical_ghg_concentrations,
    interpolate_annual_mean_to_monthly,
    make_vl_cf_inputs_that_match_historical_and_vl,
    scale_lat_gradient_based_on_emissions,
    scale_seasonality_based_on_annual_mean,
)
from cmip7_scenariomip_ghg_generation.scenario_info import ScenarioInfo


@define
class SingleConcentrationProjectionResult:
    """
    Result of running [create_scenariomip_ghgs_single_concentration_projection][]
    """

    ghg: str
    """Greenhouse gas"""

    esgf_ready_files_futures: tuple[prefect.futures.PrefectFuture[tuple[Path, ...]], ...] | None
    """ESGF-ready files futuress"""

    inverse_emissions_file_future: prefect.futures.PrefectFuture[Path]
    """Inverse emissions file future"""


def create_scenariomip_ghgs_single_concentration_projection(  # noqa: PLR0913
    ghgs: tuple[str, ...],
    harmonisation_year: int,
    scenario_infos: tuple[ScenarioInfo, ...],
    cleaned_data_path: Path,
    cmip7_historical_ghg_concentration_source_id: str,
    cmip7_historical_ghg_concentration_data_root_dir: Path,
    cmip7_historical_seasonality_lat_gradient_info_extracted: Path,
    wmo_2022_clean_file: Path,
    annual_mean_dir: Path,
    monthly_mean_dir: Path,
    seasonality_dir: Path,
    inverse_emission_dir: Path,
    lat_gradient_dir: Path,
    esgf_ready_root_dir: Path,
    esgf_files_start_year: int,
    esgf_version: str,
    esgf_institution_id: str,
    input4mips_cvs_source: str,
    doi: str,
    references_short_names: list[str],
    references_extensions_short_names: list[str],
    reference_db: Path,
    raw_notebooks_root_dir: Path,
    executed_notebooks_dir: Path,
    pool_multiprocessing: multiprocessing.pool.Pool | None,
    # Urgh
    vl_cf_name: str,
) -> dict[str, SingleConcentrationProjectionResult]:
    """
    Create the ScenarioMIP GHG concentrations for GHGs based on WMO 2022

    Parameters
    ----------
    ghgs
        Greenhouse gases for which to create output files

    harmonisation_year
        Year in which data should be harmonised to history

    cleaned_data_path
        Path in which the cleaned data is saved

    cmip7_historical_ghg_concentration_source_id
        Source ID (unique identifier) for historical CMIP7 GHG concentrations

    cmip7_historical_ghg_concentration_data_root_dir
        Root directory for saving CMIP7 historical GHG concentrations

    cmip7_historical_seasonality_lat_gradient_info_extracted
        Root directory in which the historical lat. gradient and seasonality was extracted

    wmo_2022_clean_file
        Path to the clean WMO 2022 data

        Required to handle Halon-1202, which isn't in the historical CMIP7 data for some reason.

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

    esgf_files_start_year
        Year in which ESGF files should start

    esgf_version
        Version to include in the files for ESGF

    esgf_institution_id
        Institution ID to include in the files for ESGF

    input4mips_cvs_source
        Source from which to get the input4MIPs CVs

    doi
        DOI to include in the files for ESGF

    references_short_names
        Short-names of the references that apply to these projections

    references_extensions_short_names
        Short-names of the references that apply to these projections (the extensions part)

    reference_db
        Database in which reference information is saved

    raw_notebooks_root_dir
        Root directory for raw notebooks

    executed_notebooks_dir
        Path in which to write executed notebooks

    pool_multiprocessing
        Parallel pool to use for multiprocessing

        If `None`, no parallel processing will be used

    vl_cf_name
        Hack way to identify if we're doing a `vl-cf` run

    Returns
    -------
    :
        Generated paths
    """
    downloaded_cmip7_historical_ghgs_futures = {
        ghg: submit_output_aware(
            download_cmip7_historical_ghg_concentrations,
            ghg,
            source_id=cmip7_historical_ghg_concentration_source_id,
            root_dir=cmip7_historical_ghg_concentration_data_root_dir,
            checklist_file=cmip7_historical_ghg_concentration_data_root_dir
            / f"{ghg}_{cmip7_historical_ghg_concentration_source_id}.chk",
        )
        for ghg in ghgs
        # We don't/didn't provide this for some reason,
        # even though data is there and you need it to run MAGICC
        if ghg != "halon1202"
    }

    global_mean_yearly_file_futures = {
        ghg: submit_output_aware(
            create_single_concentration_projection_annual_mean_file,
            ghg=ghg,
            cleaned_data_path=cleaned_data_path,
            historical_data_root_dir=cmip7_historical_ghg_concentration_data_root_dir,
            out_file=annual_mean_dir / f"single-concentration-projection_{ghg}_annual-mean.feather",
            raw_notebooks_root_dir=raw_notebooks_root_dir,
            executed_notebooks_dir=executed_notebooks_dir,
            wait_for=[cleaned_data_path, downloaded_cmip7_historical_ghgs_futures[ghg]]
            if ghg != "halon1202"
            else [cleaned_data_path],
        )
        for ghg in ghgs
    }

    global_mean_monthly_file_futures = {
        ghg: submit_output_aware(
            interpolate_annual_mean_to_monthly,
            ghg=ghg,
            annual_mean_file=yearly_future,
            historical_data_root_dir=cmip7_historical_ghg_concentration_data_root_dir,
            historical_data_seasonality_lat_gradient_info_root=(
                cmip7_historical_seasonality_lat_gradient_info_extracted
            ),
            wmo_2022_clean_file=wmo_2022_clean_file,
            out_file=monthly_mean_dir / f"single-concentration-projection_{ghg}_monthly-mean.nc",
            raw_notebooks_root_dir=raw_notebooks_root_dir,
            executed_notebooks_dir=executed_notebooks_dir,
        )
        for ghg, yearly_future in global_mean_yearly_file_futures.items()
    }

    seasonality_file_futures = {
        ghg: submit_output_aware(
            scale_seasonality_based_on_annual_mean,
            ghg=ghg,
            annual_mean_file=yearly_future,
            historical_data_root_dir=cmip7_historical_ghg_concentration_data_root_dir,
            historical_data_seasonality_lat_gradient_info_root=(
                cmip7_historical_seasonality_lat_gradient_info_extracted
            ),
            out_file=seasonality_dir / f"single-concentration-projection_{ghg}_seasonality-all-time.nc",
            raw_notebooks_root_dir=raw_notebooks_root_dir,
            executed_notebooks_dir=executed_notebooks_dir,
        )
        for ghg, yearly_future in global_mean_yearly_file_futures.items()
        if ghg != "halon1202"
    }

    inverse_emissions_file_futures = {
        ghg: submit_output_aware(
            calculate_inverse_emissions,
            ghg=ghg,
            monthly_mean_file=monthly_future,
            out_file=inverse_emission_dir / f"single-concentration-projection_{ghg}_inverse-emissions.feather",
            raw_notebooks_root_dir=raw_notebooks_root_dir,
            executed_notebooks_dir=executed_notebooks_dir,
            pool=pool_multiprocessing,
        )
        for ghg, monthly_future in global_mean_monthly_file_futures.items()
    }

    lat_gradient_file_futures = {
        ghg: submit_output_aware(
            scale_lat_gradient_based_on_emissions,
            ghg=ghg,
            harmonisation_year=harmonisation_year,
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
        if ghg != "halon1202"
    }

    if any(si.cmip_scenario_name == vl_cf_name for si in scenario_infos):
        if len(scenario_infos) != 1:
            msg = "Haven't figured out how to produce vl-cf at the same time as other scenarios yet"
            raise NotImplementedError(msg)

        for ghg in inverse_emissions_file_futures:
            if ghg == "halon1202":
                continue

            global_mean_monthly_file_futures[ghg], seasonality_file_futures[ghg], lat_gradient_file_futures[ghg] = (
                submit_output_aware(
                    make_vl_cf_inputs_that_match_historical_and_vl,
                    ghg=ghg,
                    internal_processing_scenario_name="all",
                    esgf_files_start_year=esgf_files_start_year,
                    monthly_mean_dir=monthly_mean_dir,
                    seasonality_dir=seasonality_dir,
                    lat_gradient_dir=lat_gradient_dir,
                    historical_data_seasonality_lat_gradient_info_root=(
                        cmip7_historical_seasonality_lat_gradient_info_extracted
                    ),
                    wmo_2022_clean_file=wmo_2022_clean_file,
                    out_file_global_mean_monthly=monthly_mean_dir
                    / f"modelling-based-projection_{ghg}_monthly-mean_vl-cf-hack.nc",
                    out_file_seasonality=seasonality_dir
                    / f"modelling-based-projection_{ghg}_seasonality-all-time_vl-cf-hack.nc",
                    out_file_lat_gradient=lat_gradient_dir / f"{ghg}_latitudinal-gradient-info_vl-cf-hack.nc",
                    raw_notebooks_root_dir=raw_notebooks_root_dir,
                    executed_notebooks_dir=executed_notebooks_dir,
                    # Don't know how to make this work within prefect's framework,
                    # hence calling.result here
                ).result()
            )

    esgf_ready_futures = {
        ghg: submit_output_aware(
            create_esgf_files,
            ghg=ghg,
            cmip_scenario_name=si.cmip_scenario_name,
            internal_processing_scenario_name="all",
            esgf_version=esgf_version,
            esgf_institution_id=esgf_institution_id,
            input4mips_cvs_source=input4mips_cvs_source,
            doi=doi,
            global_mean_monthly_file=global_mean_monthly_file_futures[ghg],
            seasonality_file=seasonality_file_futures[ghg],
            lat_gradient_file=lat_gradient_file_futures[ghg],
            esgf_ready_root_dir=esgf_ready_root_dir,
            esgf_files_start_year=esgf_files_start_year,
            historical_data_root_dir=cmip7_historical_ghg_concentration_data_root_dir,
            references_short_names=references_short_names,
            references_extensions_short_names=references_extensions_short_names,
            reference_db=reference_db,
            raw_notebooks_root_dir=raw_notebooks_root_dir,
            executed_notebooks_dir=executed_notebooks_dir,
            checklist_file=esgf_ready_root_dir / f"{ghg}_{si.cmip_scenario_name}.chk",
            pool=pool_multiprocessing,
        )
        for ghg, si in itertools.product(global_mean_monthly_file_futures, scenario_infos)
        # TODO: sort out whether we should include halon1202 in a future dataset
        if ghg != "halon1202"
    }

    res = {
        ghg: SingleConcentrationProjectionResult(
            ghg=ghg,
            esgf_ready_files_futures=(esgf_ready_futures[ghg] if ghg != "halon1202" else None,),
            inverse_emissions_file_future=inverse_emissions_file_futures[ghg],
        )
        for ghg in inverse_emissions_file_futures
    }

    return res
