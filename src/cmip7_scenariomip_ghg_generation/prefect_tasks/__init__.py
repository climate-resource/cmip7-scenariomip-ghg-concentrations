"""
Definition of [prefect](https://docs.prefect.io/v3/get-started) tasks
"""

from __future__ import annotations

from cmip7_scenariomip_ghg_generation.prefect_tasks.annual_mean_to_monthly import interpolate_annual_mean_to_monthly
from cmip7_scenariomip_ghg_generation.prefect_tasks.cmip7_historical_ghgs import (
    download_cmip7_historical_ghg_concentrations,
)
from cmip7_scenariomip_ghg_generation.prefect_tasks.download_file import download_file
from cmip7_scenariomip_ghg_generation.prefect_tasks.unzip_file import extract_tar
from cmip7_scenariomip_ghg_generation.prefect_tasks.wmo_2022 import (
    create_wmo_based_annual_mean_file,
    extract_wmo_data,
    get_wmo_ghgs,
)

__all__ = [
    "create_wmo_based_annual_mean_file",
    "download_cmip7_historical_ghg_concentrations",
    "download_file",
    "extract_tar",
    "extract_wmo_data",
    "get_wmo_ghgs",
    "interpolate_annual_mean_to_monthly",
]
