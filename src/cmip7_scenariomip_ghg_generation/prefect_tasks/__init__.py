"""
Definition of [prefect](https://docs.prefect.io/v3/get-started) tasks
"""

from __future__ import annotations

from cmip7_scenariomip_ghg_generation.prefect_tasks.annual_mean_to_monthly import interpolate_annual_mean_to_monthly
from cmip7_scenariomip_ghg_generation.prefect_tasks.cmip7_historical_ghgs import (
    download_cmip7_historical_ghg_concentrations,
)
from cmip7_scenariomip_ghg_generation.prefect_tasks.create_esgf_files import create_esgf_files
from cmip7_scenariomip_ghg_generation.prefect_tasks.download_file import download_file
from cmip7_scenariomip_ghg_generation.prefect_tasks.extraction import extract_tar, extract_zip
from cmip7_scenariomip_ghg_generation.prefect_tasks.inverse_emissions import (
    calculate_inverse_emissions,
    compile_inverse_emissions,
)
from cmip7_scenariomip_ghg_generation.prefect_tasks.magicc_handling import get_magicc_version_info
from cmip7_scenariomip_ghg_generation.prefect_tasks.make_complete_scenarios import make_complete_scenario
from cmip7_scenariomip_ghg_generation.prefect_tasks.plotting import plot_marker_overview
from cmip7_scenariomip_ghg_generation.prefect_tasks.scale_latitudinal_gradient_with_emissions import (
    scale_lat_gradient_based_on_emissions,
)
from cmip7_scenariomip_ghg_generation.prefect_tasks.scale_seasonality_with_annual_mean import (
    scale_seasonality_based_on_annual_mean,
)
from cmip7_scenariomip_ghg_generation.prefect_tasks.scm_running import run_magicc
from cmip7_scenariomip_ghg_generation.prefect_tasks.single_concentration_projection import (
    create_single_concentration_projection_annual_mean_file,
)
from cmip7_scenariomip_ghg_generation.prefect_tasks.split_input_emissions import (
    split_input_emissions_into_individual_files,
)
from cmip7_scenariomip_ghg_generation.prefect_tasks.western_et_al_2024 import (
    clean_western_et_al_2024_data,
    extend_western_et_al_2024,
    get_western_et_al_2024_clean,
)
from cmip7_scenariomip_ghg_generation.prefect_tasks.wmo_2022 import clean_wmo_data
from cmip7_scenariomip_ghg_generation.prefect_tasks.zenodo import get_doi

__all__ = [
    "calculate_inverse_emissions",
    "clean_western_et_al_2024_data",
    "clean_wmo_data",
    "compile_inverse_emissions",
    "create_esgf_files",
    "create_single_concentration_projection_annual_mean_file",
    "download_cmip7_historical_ghg_concentrations",
    "download_file",
    "extend_western_et_al_2024",
    "extract_tar",
    "extract_zip",
    "get_doi",
    "get_magicc_version_info",
    "get_western_et_al_2024_clean",
    "interpolate_annual_mean_to_monthly",
    "make_complete_scenario",
    "plot_marker_overview",
    "run_magicc",
    "scale_lat_gradient_based_on_emissions",
    "scale_seasonality_based_on_annual_mean",
    "split_input_emissions_into_individual_files",
]
