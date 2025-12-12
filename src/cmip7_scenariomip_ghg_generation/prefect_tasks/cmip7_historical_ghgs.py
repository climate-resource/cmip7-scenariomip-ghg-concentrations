"""
CMIP7 historical GHG concentrations processing tasks
"""

from __future__ import annotations

from pathlib import Path

import pooch
from prefect.tasks import exponential_backoff

from cmip7_scenariomip_ghg_generation.prefect_helpers import (
    create_hash_dict,
    task_standard_path_cache,
    write_hash_dict_to_file,
)


@task_standard_path_cache(
    task_run_name="download-cmip7-historical-ghg-concentrations_{ghg}_{source_id}",
    parameters_output=("checklist_file",),
    retries=7,
    retry_delay_seconds=exponential_backoff(backoff_factor=2),
)
def download_cmip7_historical_ghg_concentrations(
    ghg: str, source_id: str, root_dir: Path, checklist_file: Path
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

    checklist_file
        File in which to write a checklist of downloaded files
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
        ("mon", "gm", "175001-202212"),  # global monthly-mean
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
            ]
        )
        out_name = f"{ghg}_input4MIPs_GHGConcentrations_CMIP_{source_id}_{grid}_{time_frame}.nc"
        download_url = "/".join(
            [
                "https://esgf1.dkrz.de/thredds/fileServer/input4mips",
                out_path,
                out_name,
            ]
        )

        out_path_full = root_dir / out_path
        out_path_full.mkdir(exist_ok=True, parents=True)
        pooch.retrieve(
            download_url,
            known_hash=None,  # from ESGF, assume safe
            fname=out_name,
            path=out_path_full,
            progressbar=True,
        )

    write_hash_dict_to_file(
        hash_dict=create_hash_dict(root_dir.rglob(f"**/{source_id}/**/{ghg}/**/*.nc")),
        checklist_file=checklist_file,
        relative_to=root_dir,
    )
