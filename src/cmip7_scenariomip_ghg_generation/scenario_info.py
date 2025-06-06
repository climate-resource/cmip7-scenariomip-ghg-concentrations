"""
Scenario info class
"""

from attrs import frozen


@frozen
class ScenarioInfo:
    """
    Scenario information
    """

    cmip_scenario_name: str | None
    """
    Name of the scenario as used in CMIP

    If `None`, assume this is not a CMIP marker scenario
    """

    model: str
    """
    Model (IAM) that produced the scenario
    """

    scenario: str
    """
    Scenario name according to the IAM
    """
