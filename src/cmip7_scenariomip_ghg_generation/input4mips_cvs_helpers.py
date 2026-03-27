"""
Helpers for generating input4MIPs CVs information
"""


def create_source_id(esgf_institution_id: str, cmip_scenario_name: str, esgf_version: str) -> str:
    """
    Create source ID from component parts

    Parameters
    ----------
    esgf_institution_id
        Institution ID to use on ESGF

    cmip_scenario_name
        Name of the CMIP scenario

    esgf_version
        Version to use for the dataset on ESGF

    Returns
    -------
    :
        Generated source ID
    """
    source_id = f"{esgf_institution_id}-{cmip_scenario_name}-{esgf_version.replace('.', '-')}"

    return source_id


def create_source_id_extension(source_id: str, cmip_scenario_name: str) -> str:
    """
    Create source ID for the extension from the source ID for the scenario

    Parameters
    ----------
    source_id
        Source ID for the scenario (pre-extensions)

    cmip_scenario_name
        Name of the CMIP scenario

    Returns
    -------
    :
        Generated source ID
    """
    source_id = source_id.replace(cmip_scenario_name, f"{cmip_scenario_name}-ext")

    return source_id
