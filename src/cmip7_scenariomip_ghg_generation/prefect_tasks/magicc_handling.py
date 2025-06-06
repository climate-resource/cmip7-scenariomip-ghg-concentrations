"""
Support for handling MAGICC
"""

from __future__ import annotations

import platform
from pathlib import Path

from attrs import define
from prefect import task
from prefect.cache_policies import INPUTS, TASK_SOURCE


@define
class MAGICCVersionInfo:
    """Information about a MAGICC version"""

    version: str
    """Version"""

    executable: Path
    """Path to the executable"""

    probabilistic_distribution: Path
    """Path to the probabilistic distribution"""


@task(task_run_name="get-magicc-version-info_{version}", persist_result=True, cache_policy=INPUTS + TASK_SOURCE)
def get_magicc_version_info(version: str, root_folder: Path) -> MAGICCVersionInfo:  # noqa: PLR0912
    """
    Get MAGICC version information

    Parameters
    ----------
    version
        MAGICC version

    root_folder
        Root folder in which MAGICC pieces live

    Returns
    -------
    :
        Information for the given MAGICC version
    """
    if version == "MAGICCv7.6.0a3":
        if platform.system() == "Darwin":
            if platform.processor() == "arm":
                exe = root_folder / "magicc-v7.6.0a3" / "bin" / "magicc-darwin-arm64"
            else:
                raise NotImplementedError(platform.processor())

        elif platform.system() == "Windows":
            raise NotImplementedError(platform.system())

        elif platform.system().lower().startswith("linux"):
            # TODO: be fancier and allow auto-download from GitLab
            exe = root_folder / "magicc-v7.6.0a3" / "bin" / "magicc"

        else:
            raise NotImplementedError(platform.system())

        prob_distribution = root_folder / "magicc-v7.6.0a3" / "configs" / "magicc-ar7-fast-track-drawnset-v0-3-0.json"

    elif version == "MAGICCv7.5.3":
        # TODO: be fancier and allow auto-download from user token from magicc.org
        if platform.system() == "Darwin":
            if platform.processor() == "arm":
                magicc_exe = "magicc-darwin-arm64"
            else:
                raise NotImplementedError(platform.processor())

        elif platform.system() == "Windows":
            magicc_exe = "magicc.exe"

        elif platform.system().lower().startswith("linux"):
            magicc_exe = "magicc"

        else:
            raise NotImplementedError(platform.system())

        exe = root_folder / "magicc-v7.5.3" / "bin" / magicc_exe
        prob_distribution = (
            root_folder / "magicc-v7.5.3" / "configs" / "0fd0f62-derived-metrics-id-f023edb-drawnset.json"
        )

    else:
        raise NotImplementedError(version)

    res = MAGICCVersionInfo(version=version, executable=exe, probabilistic_distribution=prob_distribution)

    return res
