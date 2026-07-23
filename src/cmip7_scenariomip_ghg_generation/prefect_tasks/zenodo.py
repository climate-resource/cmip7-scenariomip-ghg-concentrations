"""
Zenodo related tasks
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from openscm_zenodo.zenodo import ZenodoDomain, ZenodoInteractor, get_reserved_doi

from cmip7_scenariomip_ghg_generation.prefect_helpers import task_basic_cache, task_standard_path_cache


@task_basic_cache(task_run_name="get-doi")
def get_doi(any_deposition_id: str) -> str:
    """
    Get DOI from Zenodo

    Parameters
    ----------
    any_deposition_id
        Any deposition ID in the Zenodo series

    Returns
    -------
    :
        DOI of draft deposit
    """
    return "10.5281/zenodo.21501391"
    try:
        zenoodo_interactor = ZenodoInteractor(
            token=os.environ["ZENODO_TOKEN"],
            zenodo_domain=ZenodoDomain.production.value,
        )
    except KeyError:
        msg = "==============\nNo zenodo token provided, DOI will just be a placeholder\n=============="
        print(msg)
        return "no-zenodo-token"

    latest_deposition_id = zenoodo_interactor.get_latest_deposition_id(
        any_deposition_id=any_deposition_id,
    )
    draft_deposition_id = zenoodo_interactor.get_draft_deposition_id(latest_deposition_id=latest_deposition_id)

    metadata = zenoodo_interactor.get_metadata(latest_deposition_id, user_controlled_only=True)
    for k in ["version"]:
        if k in metadata["metadata"]:
            metadata["metadata"].pop(k)

    update_metadata_response = zenoodo_interactor.update_metadata(
        deposition_id=draft_deposition_id,
        metadata=metadata,
    )

    doi = get_reserved_doi(update_metadata_response)

    return doi


@task_standard_path_cache(
    task_run_name="write_zenodo_json",
    parameters_output=("out_path",),
    # refresh_cache=True,
)
def write_zenodo_json(
    in_zenodo_json: Path,
    out_path: Path,
    version: str,
) -> Path:
    """
    Write zenodo JSON, updating metadata along the way

    Parameters
    ----------
    in_zenodo_json
        Input `zenodo.json` file

    out_path
        Output path to write the updated `zenodo.json` into

    version
        Version to put in the updated `zenodo.json`

    Returns
    -------
    :
        Written path
    """
    with open(in_zenodo_json) as fh:
        zenodo_raw = json.load(fh)

    zenodo_raw["metadata"]["version"] = version
    with open(out_path, "w") as fh:
        json.dump(zenodo_raw, fh)
        fh.write("\n")

    return out_path
