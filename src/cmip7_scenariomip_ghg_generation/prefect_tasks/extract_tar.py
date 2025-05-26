"""
Unzip a file
"""

from __future__ import annotations

import tarfile
from pathlib import Path

from cmip7_scenariomip_ghg_generation.prefect_helpers import task_standard_cache


@task_standard_cache(task_run_name="extract-file_{tar_file}")
def extract_tar(tar_file: Path, extract_root_dir: Path) -> Path:
    """
    Extract a tar file

    Parameters
    ----------
    tar_file
        File to extract

    extract_root_dir
        Root directory in which to extract
    """
    with tarfile.open(tar_file, "r:gz") as tar:
        tar.extractall(extract_root_dir)  # noqa: S202 # downloaded ourself

    return extract_root_dir
