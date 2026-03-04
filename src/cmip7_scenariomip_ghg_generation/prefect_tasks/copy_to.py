"""
Copy inputs to the output bundle
"""

from __future__ import annotations

import shutil
from pathlib import Path

from cmip7_scenariomip_ghg_generation.prefect_helpers import task_standard_path_cache


@task_standard_path_cache(
    task_run_name="copy-inputs-to-output-bundle_{to_copy}",
    parameters_output=("out_path",),
    # refresh_cache=True,
)
def copy_to(
    to_copy: Path,
    out_path: Path,
) -> Path:
    """
    Copy a file to a given output

    Parameters
    ----------
    to_copy
        File or directory to copy

    out_path
        Output path to copy into

    Returns
    -------
    :
        Written path
    """
    if to_copy.is_file():
        shutil.copy2(to_copy, out_path)

    elif to_copy.is_dir():
        shutil.copytree(to_copy, out_path, dirs_exist_ok=True)

    else:
        msg = f"{to_copy.exists()=}"
        raise ValueError(msg)

    return out_path
