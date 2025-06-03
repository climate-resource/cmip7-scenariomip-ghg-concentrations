"""
Definition of [prefect](https://docs.prefect.io/v3/get-started) tasks
"""

from __future__ import annotations

from cmip7_scenariomip_ghg_generation.prefect_tasks.annual_mean_to_monthly import interpolate_annual_mean_to_monthly
from cmip7_scenariomip_ghg_generation.prefect_tasks.cmip7_historical_ghgs import (
    download_cmip7_historical_ghg_concentrations,
)
from cmip7_scenariomip_ghg_generation.prefect_tasks.download_file import download_file
from cmip7_scenariomip_ghg_generation.prefect_tasks.extraction import extract_tar, extract_zip
from cmip7_scenariomip_ghg_generation.prefect_tasks.scale_seasonality_with_annual_mean import (
    scale_seasonality_based_on_annual_mean,
)
from cmip7_scenariomip_ghg_generation.prefect_tasks.single_concentration_projection import (
    create_single_concentration_projection_annual_mean_file,
)
from cmip7_scenariomip_ghg_generation.prefect_tasks.western_et_al_2024 import (
    clean_western_et_al_2024_data,
    extend_western_et_al_2024,
)
from cmip7_scenariomip_ghg_generation.prefect_tasks.wmo_2022 import clean_wmo_data

__all__ = [
    "clean_western_et_al_2024_data",
    "clean_wmo_data",
    "create_single_concentration_projection_annual_mean_file",
    "download_cmip7_historical_ghg_concentrations",
    "download_file",
    "extend_western_et_al_2024",
    "extract_tar",
    "extract_zip",
    "interpolate_annual_mean_to_monthly",
    "scale_seasonality_based_on_annual_mean",
]
