"""
Main flow
"""

from pathlib import Path

from prefect import flow

from cmip7_scenariomip_ghg_generation.prefect_tasks import extract_wmo_data


def create_scenariomip_ghgs(
    wmo_raw_data_path: Path, wmo_extracted_data_path: Path
) -> tuple[Path, ...]:
    """
    Create the ScenarioMIP GHG concentrations

    Parameters
    ----------
    wmo_raw_data_path
        Path to raw WMO data

    wmo_extracted_data_path
        Path in which to extract the WMO data

    Returns
    -------
    :
        Generated paths
    """
    extracted_wmo_data_path = extract_wmo_data(
        raw_data_path=wmo_raw_data_path, out_file=wmo_extracted_data_path
    )


def create_scenariomip_ghgs_wrapper(
    run_id: str, wmo_raw_data_path: Path, wmo_extracted_data_path: Path
) -> tuple[Path, ...]:
    """
    Create ScenarioMIP GHGs via a convenience wrapper

    The wrapper enables simpler injection of the run ID into the flow

    Parameters
    ----------
    run_id
        Run ID to use with the [create_scenariomip_ghgs][] flow

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
    run_id_flow = flow(name=f"scenariomip-ghgs_{run_id}")(create_scenariomip_ghgs)

    run_id_flow(
        wmo_raw_data_path=wmo_raw_data_path,
        wmo_extracted_data_path=wmo_extracted_data_path,
    )
