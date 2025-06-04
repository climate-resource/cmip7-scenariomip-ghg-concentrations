"""
Download a file
"""

from __future__ import annotations

from pathlib import Path

import pooch

from cmip7_scenariomip_ghg_generation.prefect_helpers import task_path_cache


@task_path_cache(
    task_run_name="download-file_{url}_{out_path}",
)
def download_file(url: Path, out_path: Path) -> Path:
    """
    Download a file with pooch

    Parameters
    ----------
    url
        File to download from

    out_path
        Path in which to save the file

    Returns
    -------
    :
        Path in which the file was saved (i.e. `out_path`)
    """
    pooch.retrieve(
        url,
        known_hash=None,  # from ESGF, assume safe
        fname=out_path.name,
        path=out_path.parent,
        progressbar=True,
    )

    return out_path
