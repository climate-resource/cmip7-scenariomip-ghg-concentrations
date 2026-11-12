"""
Re-write the EO-update CO2 and CH4 files with their final metadata

Only the file-writing notebook is run,
using the same inputs as the run recorded in the bundle's executed notebooks,
so the data does not change.
The output goes into a new directory, existing files are not touched.

This doesn't go through the main flow because the flow
would also process other gases and interact with Zenodo.
"""

from __future__ import annotations

import ast
import json
import shutil
import urllib.request
from pathlib import Path
from typing import Any

from cmip7_scenariomip_ghg_generation.notebook_running import run_notebook
from cmip7_scenariomip_ghg_generation.prefect_tasks.references import save_reference_info_to_db
from cmip7_scenariomip_ghg_generation.references import NICHOLLS_ET_AL_HISTORICAL, NICHOLLS_ET_AL_HISTORICAL_EO_UPDATE

REPO_ROOT_DIR = Path(__file__).parents[1]
BUNDLE_DIR = REPO_ROOT_DIR / "output-bundles" / "local-fit-satweighted"
OUT_DIR = BUNDLE_DIR / "data" / "processed" / "esgf-ready-eo-update"
EXECUTED_NOTEBOOKS_DIR_IN = BUNDLE_DIR / "notebooks-executed"
EXECUTED_NOTEBOOKS_DIR_OUT = BUNDLE_DIR / "notebooks-executed-eo-update"
CV_DIR = BUNDLE_DIR / "data" / "interim" / "cvs-eo-update"
REFERENCE_DB_IN = BUNDLE_DIR / "data" / "interim" / "references.db"
REFERENCE_DB_OUT = BUNDLE_DIR / "data" / "interim" / "references-eo-update.db"

GHGS = ("co2", "ch4")
SCENARIOS = ("vl", "ln", "l", "ml", "m", "hl", "h")

BASE_CVS_COMMIT = "99559118328e486dcc7243dc7a9974fa24d480df"
BASE_CVS_URL = f"https://raw.githubusercontent.com/PCMDI/input4MIPs_CVs/{BASE_CVS_COMMIT}/CVs"
CV_FILES = (
    "input4MIPs_DRS.json",
    "input4MIPs_activity_id.json",
    "input4MIPs_dataset_category.json",
    "input4MIPs_institution_id.json",
    "input4MIPs_license.json",
    "input4MIPs_mip_era.json",
    "input4MIPs_product.json",
    "input4MIPs_publication_status.json",
    "input4MIPs_required_global_attributes.json",
    "input4MIPs_source_id.json",
    "input4MIPs_target_mip.json",
    "input4MIPs_tracking_id.json",
)

MIP_ERA = "CMIP7Plus"
SOURCE_VERSION = "CMIP7Plus1.1.0"
CONTACT = (
    "anna.lanteri@climate-resource.com;"
    "zebedee.nicholls@climate-resource.com;"
    "mika.pflueger@climate-resource.com;"
    "malte.meinshausen@climate-resource.com"
)
DOI = "10.5281/zenodo.23956396"
EXTRA_GLOBAL_ATTRIBUTES = {
    "historical_data_doi": "10.5281/zenodo.23924451",
    "historical_data_description": (
        "historical concentrations with added EO-information: not suitable for production runs"
    ),
}


def get_source_ids(scenario: str) -> tuple[str, str]:
    """Get the source IDs for a scenario's main files and its extension files"""
    return f"CR-CMIP7Plus-EO-update-{scenario}-1-1-0", f"CR-CMIP7Plus-EO-update-{scenario}-ext-1-1-0"


def write_cvs() -> Path:
    """
    Write the CVs for this run

    These are the CVs used for the original run,
    plus the new MIP era and new source IDs.
    """
    CV_DIR.mkdir(parents=True, exist_ok=True)
    for name in CV_FILES:
        with urllib.request.urlopen(f"{BASE_CVS_URL}/{name}") as response:  # noqa: S310
            (CV_DIR / name).write_bytes(response.read())

    mip_era_file = CV_DIR / "input4MIPs_mip_era.json"
    mip_eras = json.loads(mip_era_file.read_text())
    mip_era_file.write_text(json.dumps([*mip_eras, MIP_ERA], indent=4) + "\n")

    source_id_file = CV_DIR / "input4MIPs_source_id.json"
    source_ids = json.loads(source_id_file.read_text())
    for scenario in SCENARIOS:
        for old, new in zip((f"CR-{scenario}-1-1-0", f"CR-{scenario}-ext-1-1-0"), get_source_ids(scenario)):
            # The authors aren't written into the files, only the contact is
            entry = {k: v for k, v in source_ids[old].items() if k != "authors"}
            source_ids[new] = {**entry, "contact": CONTACT, "mip_era": MIP_ERA, "source_version": SOURCE_VERSION}

    source_id_file.write_text(json.dumps(source_ids, indent=4) + "\n")

    return CV_DIR


def get_original_parameters(ghg: str, scenario: str) -> dict[str, Any]:
    """Get the parameters that were used when the files were originally written"""
    notebook = EXECUTED_NOTEBOOKS_DIR_IN / f"1100_create-esgf-files_{ghg}_{scenario}.ipynb"
    cells = json.loads(notebook.read_text())["cells"]
    injected = next(c for c in cells if "injected-parameters" in c["metadata"].get("tags", []))

    res = {}
    for line in "".join(injected["source"]).splitlines():
        if line.startswith("#") or " = " not in line:
            continue

        key, value = line.split(" = ", maxsplit=1)
        res[key] = ast.literal_eval(value)

    return res


def swap_nicholls_reference(short_names: list[str]) -> list[str]:
    """Swap the historical Nicholls reference for the one used in the EO-update historical files"""
    if NICHOLLS_ET_AL_HISTORICAL.short_name not in short_names:
        raise AssertionError(short_names)

    return [
        NICHOLLS_ET_AL_HISTORICAL_EO_UPDATE.short_name if v == NICHOLLS_ET_AL_HISTORICAL.short_name else v
        for v in short_names
    ]


def main() -> None:
    """Write the files"""
    if OUT_DIR.exists() and any(OUT_DIR.rglob("*.nc")):
        msg = f"{OUT_DIR} already contains files, not mixing runs"
        raise FileExistsError(msg)

    cv_dir = write_cvs()

    shutil.copy2(REFERENCE_DB_IN, REFERENCE_DB_OUT)
    save_reference_info_to_db(db=REFERENCE_DB_OUT, reference_info=(NICHOLLS_ET_AL_HISTORICAL_EO_UPDATE,))

    for ghg in GHGS:
        for scenario in SCENARIOS:
            print(f"Writing {ghg} {scenario}", flush=True)
            parameters = get_original_parameters(ghg=ghg, scenario=scenario)
            source_id, source_id_extension = get_source_ids(scenario)
            parameters.update(
                doi=DOI,
                input4mips_cvs_source=str(cv_dir),
                esgf_ready_root_dir=str(OUT_DIR),
                reference_db=str(REFERENCE_DB_OUT),
                references_short_names=swap_nicholls_reference(parameters["references_short_names"]),
                references_extensions_short_names=swap_nicholls_reference(
                    parameters["references_extensions_short_names"]
                ),
                output_filename_suffix=None,
                extra_comment=None,
                source_id_override=source_id,
                source_id_extension_override=source_id_extension,
                extra_global_attributes=EXTRA_GLOBAL_ATTRIBUTES,
            )
            run_notebook(
                notebook=REPO_ROOT_DIR / "notebooks" / "1100_create-esgf-files.py",
                run_notebooks_dir=EXECUTED_NOTEBOOKS_DIR_OUT,
                parameters=parameters,
                identity=f"{ghg}_{scenario}",
            )


if __name__ == "__main__":
    main()
