"""
Definition of [prefect](https://docs.prefect.io/v3/get-started) tasks
"""

from __future__ import annotations

from cmip7_scenariomip_ghg_generation.prefect_tasks.wmo_2022 import (
    extract_wmo_data,
    get_wmo_ghgs,
)

__all__ = ["extract_wmo_data", "get_wmo_ghgs"]
