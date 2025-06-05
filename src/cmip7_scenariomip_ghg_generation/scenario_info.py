"""
Scenario info class
"""

from attrs import define


@define
class ScenarioInfo:
    """
    Scenario information
    """

    cmip_scenario_name: str
    """
    Name of the scenario as used in CMIP
    """

    model: str
    """
    Model (IAM) that produced the scenario
    """

    scenario: str
    """
    Scenario name according to the IAM
    """
