"""
Main flow
"""

from functools import partial
from pathlib import Path

from prefect import flow
from prefect.task_runners import ThreadPoolTaskRunner
from prefect_dask import DaskTaskRunner

from cmip7_scenariomip_ghg_generation.prefect_tasks import (
    download_file,
    extract_tar,
)
from cmip7_scenariomip_ghg_generation.wmo_2022_flow import create_scenariomip_ghgs_wmo_2022_based


def create_scenariomip_ghgs_flow(  # noqa: PLR0913
    ghgs: tuple[str, ...],
    raw_notebooks_root_dir: Path,
    executed_notebooks_dir: Path,
    cmip7_historical_ghg_concentration_source_id: str,
    cmip7_historical_ghg_concentration_data_root_dir: Path,
    cmip7_historical_seasonality_lat_gradient_info_raw_file_url: str,
    cmip7_historical_seasonality_lat_gradient_info_raw_file: Path,
    cmip7_historical_seasonality_lat_gradient_info_extracted_root_dir: Path,
    wmo_raw_data_path: Path,
    wmo_extracted_data_path: Path,
    annual_mean_dir: Path,
    monthly_mean_dir: Path,
) -> tuple[Path, ...]:
    """
    Create the ScenarioMIP GHG concentrations

    Parameters
    ----------
    ghgs
        Greenhouse gases for which to create output files

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

    wmo_extracted_data_path
        Path in which to extract the WMO data

    annual_mean_dir
        Path in which to save interim annual-mean data

    monthly_mean_dir
        Path in which to save interim monthly-mean data

    Returns
    -------
    :
        Generated paths
    """
    # Used in all flows hence here
    downloaded_cmip7_historical_seasonality_lat_gradient_info = download_file.submit(
        cmip7_historical_seasonality_lat_gradient_info_raw_file_url,
        out_path=cmip7_historical_seasonality_lat_gradient_info_raw_file,
    )
    cmip7_historical_seasonality_lat_gradient_info_extracted = extract_tar.submit(
        tar_file=downloaded_cmip7_historical_seasonality_lat_gradient_info,
        extract_root_dir=cmip7_historical_seasonality_lat_gradient_info_extracted_root_dir,
    )

    wmo_2022_direct_flow_ghgs = tuple(
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

    wmo_2022_harmonised_ghgs = tuple(
        ghg
        for ghg in ghgs
        if ghg
        in [
            "hcfc141b",
            "hcfc142b",
            "hcfc22",
        ]
    )

    unsupported_ghgs = set(ghgs) - set(wmo_2022_direct_flow_ghgs) - set(wmo_2022_harmonised_ghgs)
    if unsupported_ghgs:
        msg = f"The following GHGs are not supported: {unsupported_ghgs}"
        raise AssertionError(msg)

    create_wmo_2022_based = partial(
        create_scenariomip_ghgs_wmo_2022_based,
        cmip7_historical_ghg_concentration_source_id=cmip7_historical_ghg_concentration_source_id,
        cmip7_historical_ghg_concentration_data_root_dir=cmip7_historical_ghg_concentration_data_root_dir,
        cmip7_historical_seasonality_lat_gradient_info_extracted=cmip7_historical_seasonality_lat_gradient_info_extracted,
        wmo_raw_data_path=wmo_raw_data_path,
        wmo_extracted_data_path=wmo_extracted_data_path,
        annual_mean_dir=annual_mean_dir,
        monthly_mean_dir=monthly_mean_dir,
        raw_notebooks_root_dir=raw_notebooks_root_dir,
        executed_notebooks_dir=executed_notebooks_dir,
    )
    wmo_2022_direct_flow_paths = create_wmo_2022_based(
        ghgs=wmo_2022_direct_flow_ghgs,
        harmonise=False,
    )
    wmo_2022_harmonised_paths = create_wmo_2022_based(
        ghgs=wmo_2022_harmonised_ghgs,
        harmonise=True,
    )

    # written_paths = tuple(
    #     *wmo_2022_direct_flow_paths
    #     *wmo_2022_harmonised_paths
    # )


def create_scenariomip_ghgs(  # noqa: PLR0913
    ghgs: tuple[str, ...],
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
    wmo_extracted_data_path: Path,
    annual_mean_dir: Path,
    monthly_mean_dir: Path,
) -> tuple[Path, ...]:
    """
    Create ScenarioMIP GHGs via a convenience wrapper

    The wrapper enables simpler injection of the run ID into the flow

    Parameters
    ----------
    ghgs
        Greenhouse gases for which to create output files

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

    wmo_extracted_data_path
        Path in which to save the extracted WMO data

    annual_mean_dir
        Path in which to save interim annual-mean data

    monthly_mean_dir
        Path in which to save interim monthly-mean data

    Returns
    -------
    :
        Generated paths
    """
    # A bit of trickery here to inject the run ID at runtime
    if n_workers == 1:
        task_runner = ThreadPoolTaskRunner(max_workers=1)
    else:
        task_runner = DaskTaskRunner(
            # address=  # can be used to specify an already running cluster
            cluster_kwargs={"n_workers": n_workers}
            # Other cool tricks in https://docs.prefect.io/integrations/prefect-dask
        )

    run_id_flow = flow(
        name=f"scenariomip-ghgs_{run_id}",
        task_runner=task_runner,
    )(create_scenariomip_ghgs_flow)

    run_id_flow(
        ghgs=ghgs,
        raw_notebooks_root_dir=raw_notebooks_root_dir,
        executed_notebooks_dir=executed_notebooks_dir,
        cmip7_historical_ghg_concentration_source_id=cmip7_historical_ghg_concentration_source_id,
        cmip7_historical_ghg_concentration_data_root_dir=cmip7_historical_ghg_concentration_data_root_dir,
        cmip7_historical_seasonality_lat_gradient_info_raw_file_url=cmip7_historical_seasonality_lat_gradient_info_raw_file_url,
        cmip7_historical_seasonality_lat_gradient_info_raw_file=cmip7_historical_seasonality_lat_gradient_info_raw_file,
        cmip7_historical_seasonality_lat_gradient_info_extracted_root_dir=cmip7_historical_seasonality_lat_gradient_info_extracted_root_dir,
        wmo_raw_data_path=wmo_raw_data_path,
        wmo_extracted_data_path=wmo_extracted_data_path,
        annual_mean_dir=annual_mean_dir,
        monthly_mean_dir=monthly_mean_dir,
    )
