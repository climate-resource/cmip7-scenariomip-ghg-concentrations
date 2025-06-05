"""
Create ESGF files
"""

from __future__ import annotations

from pathlib import Path

from cmip7_scenariomip_ghg_generation.notebook_running import run_notebook
from cmip7_scenariomip_ghg_generation.prefect_helpers import (
    create_hash_dict,
    task_standard_path_cache,
    write_hash_dict_to_file,
)


@task_standard_path_cache(
    task_run_name="create-esgf-files_{ghg}_{cmip_scenario_name}_{model}_{scenario}",
    parameters_output=("checklist_file",),
)
def create_esgf_files(  # noqa: PLR0913
    ghg: str,
    cmip_scenario_name: str,
    esgf_version: str,
    esgf_institution_id: str,
    input4mips_cvs_source: str,
    doi: str,
    model: str,
    scenario: str,
    global_mean_monthly_file: Path,
    seasonality_file: Path,
    lat_gradient_file: Path,
    esgf_ready_root_dir: Path,
    raw_notebooks_root_dir: Path,
    executed_notebooks_dir: Path,
    checklist_file: Path,
) -> tuple[Path, ...]:
    """
    Create ESGF files

    Parameters
    ----------
    ghg
        GHG for which to create the annual-mean

    cmip_scenario_name
        CMIP scenario name

    esgf_version
        Version to include in the files for ESGF

    esgf_institution_id
        Institution ID to include in the files for ESGF

    input4mips_cvs_source
        Source from which to get the input4MIPs CVs

    doi
        DOI to include in the files

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

    checklist_file
        File in which to write a checklist of written files

    Returns
    -------
    :
        Written paths
    """
    run_notebook(
        raw_notebooks_root_dir / "1100_create-esgf-files.py",
        parameters={
            "ghg": ghg,
            "cmip_scenario_name": cmip_scenario_name,
            "esgf_version": esgf_version,
            "esgf_institution_id": esgf_institution_id,
            "input4mips_cvs_source": input4mips_cvs_source,
            "doi": doi,
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

    esgf_ready_files = tuple(esgf_ready_root_dir.rglob(f"**/{ghg}/**/*.nc"))

    write_hash_dict_to_file(
        hash_dict=create_hash_dict(esgf_ready_files),
        checklist_file=checklist_file,
        relative_to=esgf_ready_root_dir,
    )

    return esgf_ready_files
