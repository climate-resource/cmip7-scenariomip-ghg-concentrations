"""
CMIP7 historical GHG concentrations processing tasks
"""

from __future__ import annotations

from pathlib import Path

import pooch

from cmip7_scenariomip_ghg_generation.prefect_helpers import task_standard_cache


@task_standard_cache(
    task_run_name="download-cmip7-historical-ghg-concentrations_{ghg}_{source_id}"
)
def download_cmip7_historical_ghg_concentrations(
    ghg: str, source_id: str, root_dir: Path
) -> None:
    """
    Download CMIP7 historical GHG concentration data

    Parameters
    ----------
    ghg
        GHG to download

    source_id
        Source ID to use for the download

    root_dir
        Root directory for saving the data
    """
    if source_id == "CR-CMIP-1-0-0":
        pub_date = "v20250228"
        institution_id = "CR"
    else:
        # Yuck but safe.
        # Better esgpull would remove the need for this.
        raise NotImplementedError(source_id)

    for frequency, grid, time_frame in [
        ("yr", "gm", "1750-2022"),  # global annual-mean
        ("mon", "gnz", "175001-202212"),  # monthly 15-degree
    ]:
        out_path = "/".join(
            [
                "input4MIPs",
                "CMIP7",
                "CMIP",
                institution_id,
                source_id,
                "atmos",
                frequency,
                ghg,
                grid,
                pub_date,
                f"{ghg}_input4MIPs_GHGConcentrations_CMIP_{source_id}_{grid}_{time_frame}.nc",
            ]
        )
        download_url = "/".join(
            [
                "https://esgf1.dkrz.de/thredds/fileServer/input4mips",
                out_path,
            ]
        )

        pooch.retrieve(
            download_url,
            known_hash=None,  # from ESGF, assume safe
            path=root_dir / out_path,
        )
