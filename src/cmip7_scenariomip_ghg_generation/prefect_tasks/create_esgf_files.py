"""
Create ESGF files
"""

from __future__ import annotations

from pathlib import Path

from cmip7_scenariomip_ghg_generation.notebook_running import run_notebook
from cmip7_scenariomip_ghg_generation.prefect_helpers import task_standard_cache


@task_standard_cache(task_run_name="create-esgf-files_{ghg}_{cmip_scenario_name}_{model}_{scenario}")
def create_esgf_files(  # noqa: PLR0913
    ghg: str,
    cmip_scenario_name: str,
    model: str,
    scenario: str,
    global_mean_monthly_file: Path,
    seasonality_file: Path,
    lat_gradient_file: Path,
    esgf_ready_root_dir: Path,
    raw_notebooks_root_dir: Path,
    executed_notebooks_dir: Path,
) -> tuple[Path, ...]:
    """
    Create ESGF files

    Parameters
    ----------
    ghg
        GHG for which to create the annual-mean

    cmip_scenario_name
        CMIP scenario name

    model
        Model that created the scenario

    scenario
        Name of the scenario as created by the model

    global_mean_monthly_file
        File in which the global-mean, monthly data is saved

    seasonality_file
        File in which the seasonality is saved

    lat_gradient_file
        File in which the latitudinal gradient info is saved

    esgf_ready_root_dir
        Root directory for writing ESGF-ready files

    raw_notebooks_root_dir
        Directory in which the raw notebooks live

    executed_notebooks_dir
        Directory in which executed notebooks should be written

    Returns
    -------
    :
        Written paths
    """
    run_notebook(
        raw_notebooks_root_dir / "1100_create-esgf-files.py",
        parameters={
            "esgf_version": esgf_version,
            "esgf_institution_id": esgf_institution_id,
            "input4mips_cvs_source": input4mips_cvs_source,
            "doi": doi,
            "ghg": ghg,
            "cmip_scenario_name": cmip_scenario_name,
            "model": model,
            "scenario": scenario,
            "global_mean_monthly_file": str(global_mean_monthly_file),
            "seasonality_file": str(seasonality_file),
            "lat_gradient_file": str(lat_gradient_file),
            "esgf_ready_root_dir": str(esgf_ready_root_dir),
        },
        run_notebooks_dir=executed_notebooks_dir,
        identity=f"{ghg}_{cmip_scenario_name}",
    )

    # Use grep to get written files
    esgf_ready_files = esgf_ready_root_dir.rglob(f"**/{ghg}/**")

    return esgf_ready_files
