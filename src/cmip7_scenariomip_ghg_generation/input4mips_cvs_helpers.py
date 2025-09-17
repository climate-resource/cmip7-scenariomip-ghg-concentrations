"""
Helpers for generating input4MIPs CVs information
"""


def create_source_id(esgf_institution_id: str, cmip_scenario_name: str, esgf_version: str) -> str:
    """
    TODO: docstring
    """
    source_id = f"{esgf_institution_id}-{cmip_scenario_name}-{esgf_version.replace('.', '-')}"

    return source_id
