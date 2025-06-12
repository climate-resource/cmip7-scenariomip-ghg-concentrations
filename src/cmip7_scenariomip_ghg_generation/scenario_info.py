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

    def to_file_stem(self) -> str:
        """
        Get the file stem to use for this scenario information

        Returns
        -------
        :
            File stem
        """
        return f"{self.scenario}_{self.model}".replace(" ", "_").replace(".", "-")
