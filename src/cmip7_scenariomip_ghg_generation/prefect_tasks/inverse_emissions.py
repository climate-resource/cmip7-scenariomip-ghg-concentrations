"""
Calculate inverse emissions
"""

from __future__ import annotations

from pathlib import Path

from cmip7_scenariomip_ghg_generation.notebook_running import run_notebook
from cmip7_scenariomip_ghg_generation.prefect_helpers import task_standard_cache


@task_standard_cache(task_run_name="calculate-inverse-emissions_{ghg}")
def calculate_inverse_emissions(
    ghg: str,
    monthly_mean_file: Path,
    inverse_emission_dir: Path,
    raw_notebooks_root_dir: Path,
    executed_notebooks_dir: Path,
) -> Path:
    """
    Calculate inverse emissions

    Parameters
    ----------
    ghg
        GHG for which to create the annual-mean

    monthly_mean_file
        Path in which the monthly-mean data is written

    inverse_emission_dir
        Directory in which to write the inverse emissions file

    raw_notebooks_root_dir
        Directory in which the raw notebooks live

    executed_notebooks_dir
        Directory in which executed notebooks should be written

    Returns
    -------
    :
        Written path
    """
    if "monthly-mean" not in monthly_mean_file.name:
        raise AssertionError(monthly_mean_file.name)

    if not monthly_mean_file.name.endswith(".nc"):
        raise AssertionError(monthly_mean_file.name)

    out_file = inverse_emission_dir / monthly_mean_file.name.replace("monthly-mean", "inverse-emissions").replace(
        ".nc", ".feather"
    )

    run_notebook(
        raw_notebooks_root_dir / "1020_calculate-inverse-emissions.py",
        parameters={
            "ghg": ghg,
            "monthly_mean_file": str(monthly_mean_file),
            "out_file": str(out_file),
        },
        run_notebooks_dir=executed_notebooks_dir,
        identity=out_file.stem,
    )

    return out_file
