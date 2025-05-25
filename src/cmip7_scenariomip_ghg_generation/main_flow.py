"""
Main flow
"""

from pathlib import Path

from prefect import flow, unmapped
from prefect.task_runners import ThreadPoolTaskRunner
from prefect_dask import DaskTaskRunner

from cmip7_scenariomip_ghg_generation.prefect_tasks import (
    create_wmo_based_annual_mean_file,
    download_cmip7_historical_ghg_concentrations,
    extract_wmo_data,
    get_wmo_ghgs,
)


def create_scenariomip_ghgs_flow(  # noqa: PLR0913
    ghgs: tuple[str, ...],
    raw_notebooks_root_dir: Path,
    executed_notebooks_dir: Path,
    cmip7_historical_ghg_concentration_source_id: str,
    cmip7_historical_ghg_concentration_data_root_dir: Path,
    wmo_raw_data_path: Path,
    wmo_extracted_data_path: Path,
    annual_mean_dir: Path,
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

    wmo_raw_data_path
        Path to raw WMO data

    wmo_extracted_data_path
        Path in which to extract the WMO data

    annual_mean_dir
        Path in which to save interim annual-mean data

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

    extracted_wmo_data_path = extract_wmo_data(raw_data_path=wmo_raw_data_path, out_file=wmo_extracted_data_path)
    all_wmo_ghgs = get_wmo_ghgs(extracted_wmo_data_path)
    wmo_based_ghgs = tuple(ghg for ghg in ghgs if ghg in all_wmo_ghgs)

    wmo_based_annual_mean_files = create_wmo_based_annual_mean_file.map(
        ghg=wmo_based_ghgs,
        extracted_wmo_data_path=unmapped(extracted_wmo_data_path),
        historical_data_root_dir=unmapped(cmip7_historical_ghg_concentration_data_root_dir),
        annual_mean_dir=unmapped(annual_mean_dir),
        raw_notebooks_root_dir=unmapped(raw_notebooks_root_dir),
        executed_notebooks_dir=unmapped(executed_notebooks_dir),
        # Could make this smarter so we only wait for the GHG
        # we need, but not sure how to do that within map
        # (probably not possible)
        # and downloading is fast and cached so I am moving on with life.
        wait_for=download_cmip7_historical_ghgs,
    )

    return wmo_based_annual_mean_files.result()


def create_scenariomip_ghgs(  # noqa: PLR0913
    ghgs: tuple[str, ...],
    run_id: str,
    n_workers: int,
    raw_notebooks_root_dir: Path,
    executed_notebooks_dir: Path,
    cmip7_historical_ghg_concentration_source_id: str,
    cmip7_historical_ghg_concentration_data_root_dir: Path,
    wmo_raw_data_path: Path,
    wmo_extracted_data_path: Path,
    annual_mean_dir: Path,
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

    wmo_raw_data_path
        Path to the raw WMO data

    wmo_extracted_data_path
        Path in which to save the extracted WMO data

    annual_mean_dir
        Path in which to save interim annual-mean data

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
        wmo_raw_data_path=wmo_raw_data_path,
        wmo_extracted_data_path=wmo_extracted_data_path,
        annual_mean_dir=annual_mean_dir,
    )
