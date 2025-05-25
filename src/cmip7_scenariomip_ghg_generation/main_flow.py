"""
Main flow
"""

from pathlib import Path

from prefect import flow, unmapped
from prefect.task_runners import ThreadPoolTaskRunner
from prefect_dask import DaskTaskRunner

from cmip7_scenariomip_ghg_generation.prefect_tasks import (
    download_cmip7_historical_ghg_concentrations,
    extract_wmo_data,
    get_wmo_ghgs,
)


def create_scenariomip_ghgs_flow(
    ghgs: tuple[str, ...],
    cmip7_historical_ghg_concentration_source_id: str,
    cmip7_historical_ghg_concentration_data_root_dir: Path,
    wmo_raw_data_path: Path,
    wmo_extracted_data_path: Path,
) -> tuple[Path, ...]:
    """
    Create the ScenarioMIP GHG concentrations

    Parameters
    ----------
    ghgs
        Greenhouse gases for which to create output files

    cmip7_historical_ghg_concentration_source_id
        Source ID (unique identifier) for historical CMIP7 GHG concentrations

    cmip7_historical_ghg_concentration_data_root_dir
        Root directory for saving CMIP7 historical GHG concentrations

    wmo_raw_data_path
        Path to raw WMO data

    wmo_extracted_data_path
        Path in which to extract the WMO data

    Returns
    -------
    :
        Generated paths
    """
    download_cmip7_historical_ghgs = download_cmip7_historical_ghg_concentrations.map(
        ghgs,
        source_id=unmapped(cmip7_historical_ghg_concentration_source_id),
        root_dir=unmapped(cmip7_historical_ghg_concentration_data_root_dir),
    )

    extracted_wmo_data_path = extract_wmo_data.submit(
        raw_data_path=wmo_raw_data_path, out_file=wmo_extracted_data_path
    )

    wmo_ghgs = get_wmo_ghgs.submit(extracted_wmo_data_path)
    wmo_ghgs.result()

    # wmo_based_annual_mean_files = create_wmo_based_annual_mean_file.map(
    #     ghg=wmo_ghgs,
    #     historical_data_root_dir=cmip7_historical_ghg_concentration_data_root_dir,
    #     # Could make this smarter so we only wait for the GHG
    #     # we need, but not sure how to do that within map
    #     # (probably not possible)
    #     # and downloading is fast and cached so I am moving on with life.
    #     wait_for=download_cmip7_historical_ghgs,
    # )


def create_scenariomip_ghgs(  # noqa: PLR0913
    ghgs: tuple[str, ...],
    run_id: str,
    n_workers: int,
    cmip7_historical_ghg_concentration_source_id: str,
    cmip7_historical_ghg_concentration_data_root_dir: Path,
    wmo_raw_data_path: Path,
    wmo_extracted_data_path: Path,
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

    cmip7_historical_ghg_concentration_source_id
        Source ID (unique identifier) for historical CMIP7 GHG concentrations

    cmip7_historical_ghg_concentration_data_root_dir
        Root directory for saving CMIP7 historical GHG concentrations

    wmo_raw_data_path
        Path to the raw WMO data

    wmo_extracted_data_path
        Path in which to save the extracted WMO data

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
        cmip7_historical_ghg_concentration_source_id=cmip7_historical_ghg_concentration_source_id,
        cmip7_historical_ghg_concentration_data_root_dir=cmip7_historical_ghg_concentration_data_root_dir,
        wmo_raw_data_path=wmo_raw_data_path,
        wmo_extracted_data_path=wmo_extracted_data_path,
    )
