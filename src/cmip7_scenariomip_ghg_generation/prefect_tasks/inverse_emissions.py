"""
Calculate inverse emissions
"""

from __future__ import annotations

from pathlib import Path

from cmip7_scenariomip_ghg_generation.notebook_running import run_notebook
from cmip7_scenariomip_ghg_generation.prefect_helpers import task_path_cache


@task_path_cache(
    task_run_name="calculate-inverse-emissions_{ghg}",
    # refresh_cache=True,
)
def calculate_inverse_emissions(
    ghg: str,
    monthly_mean_file: Path,
    out_file: Path,
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

    out_file
        File in which to write the inverse emissions

    raw_notebooks_root_dir
        Directory in which the raw notebooks live

    executed_notebooks_dir
        Directory in which executed notebooks should be written

    Returns
    -------
    :
        Written path
    """
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
