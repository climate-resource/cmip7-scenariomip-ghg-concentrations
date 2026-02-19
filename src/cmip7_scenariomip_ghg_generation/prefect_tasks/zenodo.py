"""
Zenodo related tasks
"""

from __future__ import annotations

import os

from openscm_zenodo.zenodo import ZenodoDomain, ZenodoInteractor, get_reserved_doi

from cmip7_scenariomip_ghg_generation.prefect_helpers import task_basic_cache


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
    zenoodo_interactor = ZenodoInteractor(
        token=os.environ["ZENODO_TOKEN"],
        zenodo_domain=ZenodoDomain.production.value,
    )

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
