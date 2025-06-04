"""
Zenodo related tasks
"""

from __future__ import annotations

from cmip7_scenariomip_ghg_generation.prefect_helpers import task_standard_cache


@task_standard_cache(task_run_name="get-doi")
def get_doi() -> str:
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
    return "dev-test"
    # # TODO: use zenodo interaction instead
    # zenoodo_interactor = ZenodoInteractor(
    #     token=os.environ["ZENODO_TOKEN"],
    #     zenodo_domain=ZenodoDomain.production.value,
    # )
    #
    # latest_deposition_id = zenoodo_interactor.get_latest_deposition_id(
    #     any_deposition_id=any_deposition_id,
    # )
    # draft_deposition_id = zenoodo_interactor.get_draft_deposition_id(latest_deposition_id=latest_deposition_id)
    #
    # # # TODO: put this somewhere else
    # # metadata = zenoodo_interactor.get_metadata(latest_deposition_id, user_controlled_only=True)
    # # for k in ["doi", "prereserve_doi", "publication_date", "version"]:
    # #     if k in metadata["metadata"]:
    # #         metadata["metadata"].pop(k)
    # #
    # # update_metadata_response = zenoodo_interactor.update_metadata(
    # #     deposition_id=draft_deposition_id,
    # #     metadata=metadata,
    # # )
    #
    # doi = get_reserved_doi(update_metadata_response)
    #
    # return doi
