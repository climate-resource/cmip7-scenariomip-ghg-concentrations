"""
Main flow
"""

from functools import partial
from pathlib import Path

from prefect import flow
from prefect.task_runners import ThreadPoolTaskRunner

from cmip7_scenariomip_ghg_generation.prefect_tasks import (
    clean_western_et_al_2024_data,
    clean_wmo_data,
    download_file,
    extend_western_et_al_2024,
    extract_tar,
    extract_zip,
    get_doi,
)
from cmip7_scenariomip_ghg_generation.scenario_info import ScenarioInfo
from cmip7_scenariomip_ghg_generation.single_concentration_projection_flow import (
    create_scenariomip_ghgs_single_concentration_projection,
)


def create_scenariomip_ghgs_flow(  # noqa: PLR0913
    ghgs: tuple[str, ...],
    scenario_infos: tuple[ScenarioInfo, ...],
    raw_notebooks_root_dir: Path,
    executed_notebooks_dir: Path,
    cmip7_historical_ghg_concentration_source_id: str,
    cmip7_historical_ghg_concentration_data_root_dir: Path,
    cmip7_historical_seasonality_lat_gradient_info_raw_file_url: str,
    cmip7_historical_seasonality_lat_gradient_info_raw_file: Path,
    cmip7_historical_seasonality_lat_gradient_info_extracted_root_dir: Path,
    wmo_raw_data_path: Path,
    wmo_cleaned_data_path: Path,
    western_et_al_2024_download_url: str,
    western_et_al_2024_raw_tar_file: Path,
    western_et_al_2024_extract_path: Path,
    western_et_al_2024_extracted_file_of_interest: Path,
    western_et_al_2024_cleaned_data_path: Path,
    annual_mean_dir: Path,
    monthly_mean_dir: Path,
    seasonality_dir: Path,
    inverse_emission_dir: Path,
    lat_gradient_dir: Path,
    esgf_ready_root_dir: Path,
    esgf_version: str,
    esgf_institution_id: str,
    input4mips_cvs_source: str,
) -> tuple[Path, ...]:
    """
    Create the ScenarioMIP GHG concentrations

    Parameters
    ----------
    ghgs
        Greenhouse gases for which to create output files

    scenario_infos
        Scenario information

    raw_notebooks_root_dir
        Root directory for raw notebooks

    executed_notebooks_dir
        Path in which to write executed notebooks

    cmip7_historical_ghg_concentration_source_id
        Source ID (unique identifier) for historical CMIP7 GHG concentrations

    cmip7_historical_ghg_concentration_data_root_dir
        Root directory for saving CMIP7 historical GHG concentrations

    cmip7_historical_seasonality_lat_gradient_info_raw_file_url
        URL from which to download the CMIP7 historical GHG concentrations seasonality and lat. gradient info

    cmip7_historical_seasonality_lat_gradient_info_raw_file
        File in which to save the CMIP7 historical GHG concentrations seasonality and lat. gradient info

    cmip7_historical_seasonality_lat_gradient_info_extracted_root_dir
        Root directory in which to extract `cmip7_historical_seasonality_lat_gradient_info_raw_file`

    wmo_raw_data_path
        Path to raw WMO data

    wmo_cleaned_data_url
        Path in which to extract the WMO data

    western_et_al_2024_download_url
        URL from which to download the raw Western et al. (2024) data

    western_et_al_2024_raw_tar_file
        Path in which to download the raw Western et al. (2024) data

    western_et_al_2024_extract_path
        Path in which to extract the raw Western et al. (2024) data

    western_et_al_2024_extracted_file_of_interest
        File of interest from the extracted Western et al. (2024) data

    western_et_al_2024_cleaned_data_path
        Path in which to save the cleaned Western et al. (2024) data

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

    esgf_version
        Version to include in the files for ESGF

    esgf_institution_id
        Institution ID to include in the files for ESGF

    input4mips_cvs_source
        Source from which to get the input4MIPs CVs

    Returns
    -------
    :
        Generated paths
    """
    ### Used in all flows hence here
    downloaded_cmip7_historical_seasonality_lat_gradient_info = download_file.submit(
        cmip7_historical_seasonality_lat_gradient_info_raw_file_url,
        out_path=cmip7_historical_seasonality_lat_gradient_info_raw_file,
    )
    cmip7_historical_seasonality_lat_gradient_info_extracted = extract_tar.submit(
        tar_file=downloaded_cmip7_historical_seasonality_lat_gradient_info,
        extract_root_dir=cmip7_historical_seasonality_lat_gradient_info_extracted_root_dir,
    )
    doi = get_doi.submit()

    ### WMO 2022
    wmo_2022_ghgs = tuple(
        ghg
        for ghg in ghgs
        if ghg
        in [
            "ccl4",
            "cfc11",
            "cfc12",
            "cfc113",
            "cfc114",
            "cfc115",
            "ch3br",
            "ch3ccl3",
            "ch3cl",
            "halon1211",
        ]
    )

    ### Western et al. 2024
    western_et_al_2024_ghgs = tuple(
        ghg
        for ghg in ghgs
        if ghg
        in [
            "hcfc141b",
            "hcfc142b",
            "hcfc22",
        ]
    )

    unsupported_ghgs = set(ghgs) - set(wmo_2022_ghgs) - set(western_et_al_2024_ghgs)
    if unsupported_ghgs:
        msg = f"The following GHGs are not supported: {unsupported_ghgs}"
        raise AssertionError(msg)

    create_single_concentration_projection = partial(
        create_scenariomip_ghgs_single_concentration_projection,
        scenario_infos=scenario_infos,
        cmip7_historical_ghg_concentration_source_id=cmip7_historical_ghg_concentration_source_id,
        cmip7_historical_ghg_concentration_data_root_dir=cmip7_historical_ghg_concentration_data_root_dir,
        cmip7_historical_seasonality_lat_gradient_info_extracted=cmip7_historical_seasonality_lat_gradient_info_extracted,
        annual_mean_dir=annual_mean_dir,
        monthly_mean_dir=monthly_mean_dir,
        seasonality_dir=seasonality_dir,
        inverse_emission_dir=inverse_emission_dir,
        lat_gradient_dir=lat_gradient_dir,
        esgf_ready_root_dir=esgf_ready_root_dir,
        raw_notebooks_root_dir=raw_notebooks_root_dir,
        executed_notebooks_dir=executed_notebooks_dir,
        esgf_version=esgf_version,
        esgf_institution_id=esgf_institution_id,
        input4mips_cvs_source=input4mips_cvs_source,
        doi=doi,
    )

    wmo_2022_cleaned = clean_wmo_data(raw_data_path=wmo_raw_data_path, out_file=wmo_cleaned_data_path)
    wmo_2022_paths = create_single_concentration_projection(
        ghgs=wmo_2022_ghgs,
        cleaned_data_path=wmo_2022_cleaned,
    )

    western_et_al_2024_raw_tar_file_downloaded = download_file.submit(
        url=western_et_al_2024_download_url,
        out_path=western_et_al_2024_raw_tar_file,
    )
    western_et_al_2024_extracted = extract_zip.submit(
        zip_file=western_et_al_2024_raw_tar_file_downloaded,
        extract_root_dir=western_et_al_2024_extract_path,
    )

    western_et_al_2024_cleaned = clean_western_et_al_2024_data.submit(
        root_extraction_dir=western_et_al_2024_extracted,
        file_to_clean=western_et_al_2024_extracted_file_of_interest,
        out_file=western_et_al_2024_cleaned_data_path,
    )

    western_2024_paths_l = []
    for ghg in western_et_al_2024_ghgs:
        western_et_al_2024_extended_ghg = extend_western_et_al_2024.submit(
            ghg=ghg,
            western_et_al_2024_clean=western_et_al_2024_cleaned,
            wmo_2022_clean=wmo_2022_cleaned,
            raw_notebooks_root_dir=raw_notebooks_root_dir,
            executed_notebooks_dir=executed_notebooks_dir,
        )

        western_2024_paths_l.extend(
            create_single_concentration_projection(
                ghgs=[ghg],
                cleaned_data_path=western_et_al_2024_extended_ghg,
            )
        )

    # Make sure all results are crunched before returning
    for future in (*wmo_2022_paths, *western_2024_paths_l):
        future.wait(timeout=30 * 60)


def create_scenariomip_ghgs(  # noqa: PLR0913
    ghgs: tuple[str, ...],
    scenario_infos: tuple[ScenarioInfo, ...],
    run_id: str,
    n_workers: int,
    raw_notebooks_root_dir: Path,
    executed_notebooks_dir: Path,
    cmip7_historical_ghg_concentration_source_id: str,
    cmip7_historical_ghg_concentration_data_root_dir: Path,
    cmip7_historical_seasonality_lat_gradient_info_raw_file_url: str,
    cmip7_historical_seasonality_lat_gradient_info_raw_file: Path,
    cmip7_historical_seasonality_lat_gradient_info_extracted_root_dir: Path,
    wmo_raw_data_path: Path,
    wmo_cleaned_data_path: Path,
    western_et_al_2024_download_url: str,
    western_et_al_2024_raw_tar_file: Path,
    western_et_al_2024_extract_path: Path,
    western_et_al_2024_extracted_file_of_interest: Path,
    western_et_al_2024_cleaned_data_path: Path,
    annual_mean_dir: Path,
    monthly_mean_dir: Path,
    seasonality_dir: Path,
    inverse_emission_dir: Path,
    lat_gradient_dir: Path,
    esgf_ready_root_dir: Path,
    esgf_version: str,
    esgf_institution_id: str,
    input4mips_cvs_source: str,
) -> tuple[Path, ...]:
    """
    Create ScenarioMIP GHGs via a convenience wrapper

    The wrapper enables simpler injection of the run ID into the flow

    Parameters
    ----------
    ghgs
        Greenhouse gases for which to create output files

    scenario_infos
        Scenario information

    run_id
        Run ID to use with the [create_scenariomip_ghgs][] flow

    n_workers
        Number of workers to use with parallel processing

    raw_notebooks_root_dir
        Root directory for raw notebooks

    executed_notebooks_dir
        Path in which to write executed notebooks

    cmip7_historical_ghg_concentration_source_id
        Source ID (unique identifier) for historical CMIP7 GHG concentrations

    cmip7_historical_ghg_concentration_data_root_dir
        Root directory for saving CMIP7 historical GHG concentrations

    cmip7_historical_seasonality_lat_gradient_info_raw_file_url
        URL from which to download the CMIP7 historical GHG concentrations seasonality and lat. gradient info

    cmip7_historical_seasonality_lat_gradient_info_raw_file
        File in which to save the CMIP7 historical GHG concentrations seasonality and lat. gradient info

    cmip7_historical_seasonality_lat_gradient_info_extracted_root_dir
        Root directory in which to extract `cmip7_historical_seasonality_lat_gradient_info_raw_file`

    wmo_raw_data_path
        Path to the raw WMO data

    wmo_cleaned_data_path
        Path in which to save the extracted WMO data

    western_et_al_2024_download_url
        URL from which to download the raw Western et al. (2024) data

    western_et_al_2024_raw_tar_file
        Path in which to download the raw Western et al. (2024) data

    western_et_al_2024_extract_path
        Path in which to extract the raw Western et al. (2024) data

    western_et_al_2024_extracted_file_of_interest
        File of interest from the extracted Western et al. (2024) data

    western_et_al_2024_cleaned_data_path
        Path in which to save the cleaned Western et al. (2024) data

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

    esgf_version
        Version to include in the files for ESGF

    esgf_institution_id
        Institution ID to include in the files for ESGF

    input4mips_cvs_source
        Source from which to get the input4MIPs CVs

    Returns
    -------
    :
        Generated paths
    """
    # A bit of trickery here to inject the run ID at runtime
    if n_workers == 1:
        task_runner = ThreadPoolTaskRunner(max_workers=1)
    else:
        # Apparently this is not truly parallel,
        # but maybe it still works well enough for us
        # given that many of our tasks spin up their own process anyway ?
        # Docs: https://docs.prefect.io/v3/develop/task-runners
        task_runner = ThreadPoolTaskRunner(max_workers=n_workers)
        # task_runner = DaskTaskRunner(
        #     # address=  # can be used to specify an already running cluster
        #     cluster_kwargs={"n_workers": n_workers}
        #     # Other cool tricks in https://docs.prefect.io/integrations/prefect-dask
        # )

    run_id_flow = flow(
        name=f"scenariomip-ghgs_{run_id}",
        task_runner=task_runner,
    )(create_scenariomip_ghgs_flow)

    return run_id_flow(
        ghgs=ghgs,
        scenario_infos=scenario_infos,
        raw_notebooks_root_dir=raw_notebooks_root_dir,
        executed_notebooks_dir=executed_notebooks_dir,
        cmip7_historical_ghg_concentration_source_id=cmip7_historical_ghg_concentration_source_id,
        cmip7_historical_ghg_concentration_data_root_dir=cmip7_historical_ghg_concentration_data_root_dir,
        cmip7_historical_seasonality_lat_gradient_info_raw_file_url=cmip7_historical_seasonality_lat_gradient_info_raw_file_url,
        cmip7_historical_seasonality_lat_gradient_info_raw_file=cmip7_historical_seasonality_lat_gradient_info_raw_file,
        cmip7_historical_seasonality_lat_gradient_info_extracted_root_dir=cmip7_historical_seasonality_lat_gradient_info_extracted_root_dir,
        wmo_raw_data_path=wmo_raw_data_path,
        wmo_cleaned_data_path=wmo_cleaned_data_path,
        western_et_al_2024_download_url=western_et_al_2024_download_url,
        western_et_al_2024_raw_tar_file=western_et_al_2024_raw_tar_file,
        western_et_al_2024_extract_path=western_et_al_2024_extract_path,
        western_et_al_2024_extracted_file_of_interest=western_et_al_2024_extracted_file_of_interest,
        western_et_al_2024_cleaned_data_path=western_et_al_2024_cleaned_data_path,
        annual_mean_dir=annual_mean_dir,
        monthly_mean_dir=monthly_mean_dir,
        seasonality_dir=seasonality_dir,
        inverse_emission_dir=inverse_emission_dir,
        lat_gradient_dir=lat_gradient_dir,
        esgf_ready_root_dir=esgf_ready_root_dir,
        esgf_version=esgf_version,
        esgf_institution_id=esgf_institution_id,
        input4mips_cvs_source=input4mips_cvs_source,
    )
