"""
Definition of references
"""

from attrs import define


@define
class ReferenceInfo:
    """Information about a reference"""

    short_name: str
    """Short name of the reference"""

    licence: str
    """Licence applied to the reference's data"""

    reference: str
    """Reference (long text)"""

    resource_type: str
    """Resource type of the reference (used for cross-linking on Zenodo)"""

    url: str
    """URL"""

    doi: str | None = None
    """DOI"""


NICHOLLS_ET_AL_HISTORICAL = ReferenceInfo(
    short_name="Nicholls et al., historical GHG concentrations, 2026 (in-prep)",
    licence="Paper, NA",
    reference=(
        "Nicholls, Z., Meinshausen, M., Lewis, J., Pflueger, M., Menking, A., ...: "
        "Greenhouse gas concentrations for climate modelling (CMIP7), "
        "in-prep, 2025."
    ),
    url="https://github.com/climate-resource/CMIP-GHG-Concentration-Generation",
    resource_type="publication-article",
)

NICHOLLS_ET_AL_SCENARIOS = ReferenceInfo(
    short_name="Nicholls et al., future GHG concentrations, 2026 (in-prep)",
    licence="Paper, NA",
    reference=(
        "Nicholls, Z., Meinshausen, M., Lewis, J., Pflueger, M., Menking, A., ...: "
        "Future greenhouse gas concentrations for climate modelling (CMIP7 ScenarioMIP), "
        "in-prep, 2025."
    ),
    url="https://github.com/climate-resource/cmip7-scenariomip-ghg-concentrations",
    resource_type="publication-article",
)

MEINSHAUSEN_ET_AL_SCENARIOS = ReferenceInfo(
    short_name="Meinshausen et al., 2020",
    licence="Paper, NA",
    reference=(
        "Meinshausen, M., Nicholls, Z. R. J., ..., Vollmer, M. K., and Wang, R. H. J.: "
        "The shared socio-economic pathway (SSP) greenhouse gas concentrations "
        "and their extensions to 2500, "
        "Geosci. Model Dev., 13, 3571-3605, https://doi.org/10.5194/gmd-13-3571-2020, 2020."
    ),
    doi="https://doi.org/10.5194/gmd-13-3571-2020",
    url="https://doi.org/10.5194/gmd-13-3571-2020",
    resource_type="publication-article",
)

MEINSHAUSEN_ET_AL_2009 = ReferenceInfo(
    short_name="Meinshausen et al., 2009",
    licence="Paper, NA",
    reference=(
        "Meinshausen, M., Meinshausen, N., ..., Frame, D. J., & Allen, M. R.: "
        "Greenhouse-gas emission targets for limiting global warming to 2°C, "
        "Nature, 458(7242), 1158-1162, 2009."
    ),
    doi="https://doi.org/10.1038/nature08017",
    url="https://doi.org/10.1038/nature08017",
    resource_type="publication-article",
)

MEINSHAUSEN_ET_AL_2011 = ReferenceInfo(
    short_name="Meinshausen et al., 2011",
    licence="Paper, NA",
    reference=(
        "Meinshausen, M., Raper, S. C. B., & Wigley, T. M. L.: "
        "Emulating coupled atmosphere-ocean and carbon cycle models with a simpler model, MAGICC6 "
        "- Part 1: Model description and calibration, "
        "Atmospheric Chemistry and Physics, 11(4), 1417-1456, 2011."
    ),
    doi="https://doi.org/10.5194/acp-11-1417-2011",
    url="https://doi.org/10.5194/acp-11-1417-2011",
    resource_type="publication-article",
)

FORSTER_ET_AL_AR6_WG1_CH7 = ReferenceInfo(
    short_name="Forster et al, 2021",
    licence="Book chapter, NA",
    reference=(
        "Forster, P., Storelvmo, T., Armour, K., ..., and Zhang, H. "
        "(2021). "
        "Chapter 7: The Earth's Energy Budget, Climate Feedbacks, and Climate Sensitivity. "
        "In Climate Change 2021: The Physical Science Basis. "
        "Contribution of Working Group I to the Sixth Assessment Report "
        "of the Intergovernmental Panel on Climate Change "
        "(pp. 923-1054); Cambridge University Press, 2021"
    ),
    doi="https://doi.org/10.1017/9781009157896.009",
    url="https://doi.org/10.1017/9781009157896.009",
    resource_type="publication-book",
)

WMO_2022 = ReferenceInfo(
    short_name="WMO 2022",
    licence="Underlying data all openly licensed, so assuming the same, but not 100% clear",
    reference=(
        "Daniel, J. S., Reimann, S., ..., Schofield, R., Walter-Terrinoni, H. "
        "(2022). "
        "Chapter 7: Scenarios and Information for Policymakers. "
        "In World Meteorological Organization (WMO), "
        "Scientific Assessment of Ozone Depletion: 2022, GAW Report No. 278"
        "(pp. 509); WMO: Geneva, 2022."
    ),
    # Are there proper DOIs?
    doi=None,
    url="https://ozone.unep.org/sites/default/files/2023-02/Scientific-Assessment-of-Ozone-Depletion-2022.pdf",
    resource_type="publication-book",
)

WESTERN_ET_AL_2024 = ReferenceInfo(
    short_name="Western et al., 2024",
    licence="CC BY 4.0",  # https://zenodo.org/records/10782689
    reference=(
        "Western, L.M., Daniel, J.S., Vollmer, M.K. et al. "
        "A decrease in radiative forcing "
        "and equivalent effective chlorine from hydrochlorofluorocarbons. "
        "Nat. Clim. Chang. 14, 805-807 (2024)."
    ),
    doi="https://doi.org/10.1038/s41558-024-02038-7",
    url="https://doi.org/10.1038/s41558-024-02038-7",
    resource_type="publication-article",
)

SCENARIO_REFERENCES = {
    ("REMIND-MAgPIE 3.5-4.11", "vl"): ReferenceInfo(
        short_name="REMIND-MAgPIE integrated assessment modelling team, 2026 (in-prep)",
        licence="Paper, NA",
        reference=("REMIND-MAgPIE team, ...: " "High to low scenario for CMIP7 ScenarioMIP, " "in-prep, 2026."),
        url="https://zenodo.org/records/18497404",
        resource_type="publication-article",
    ),
    ("AIM 3.0", "ln"): ReferenceInfo(
        short_name="AIM integrated assessment modelling team, 2026 (in-prep)",
        licence="Paper, NA",
        reference=("AIM team, ...: " "Low to negative scenario for CMIP7 ScenarioMIP, " "in-prep, 2026."),
        url="https://zenodo.org/records/18497404",
        resource_type="publication-article",
    ),
    ("MESSAGEix-GLOBIOM-GAINS 2.1-M-R12", "l"): ReferenceInfo(
        short_name="MESSAGEix-GLOBIOM-GAINS integrated assessment modelling team, 2026 (in-prep)",
        licence="Paper, NA",
        reference=("MESSAGEix-GLOBIOM-GAINS team, ...: " "Low scenario for CMIP7 ScenarioMIP, " "in-prep, 2026."),
        url="https://zenodo.org/records/18497404",
        resource_type="publication-article",
    ),
    ("COFFEE 1.6", "ml"): ReferenceInfo(
        short_name="COFFEE integrated assessment modelling team, 2026 (in-prep)",
        licence="Paper, NA",
        reference=("COFFEE team, ...: " "Medium scenario for CMIP7 ScenarioMIP, " "in-prep, 2026."),
        url="https://zenodo.org/records/18497404",
        resource_type="publication-article",
    ),
    ("IMAGE 3.4", "m"): ReferenceInfo(
        short_name="IMAGE integrated assessment modelling team, 2026 (in-prep)",
        licence="Paper, NA",
        reference=("IMAGE team, ...: " "Medium scenario for CMIP7 ScenarioMIP, " "in-prep, 2026."),
        url="https://zenodo.org/records/18497404",
        resource_type="publication-article",
    ),
    ("WITCH 6.0", "hl"): ReferenceInfo(
        short_name="WITCH integrated assessment modelling team, 2026 (in-prep)",
        licence="Paper, NA",
        reference=("WITCH team, ...: " "High to low scenario for CMIP7 ScenarioMIP, " "in-prep, 2026."),
        url="https://zenodo.org/records/18497404",
        resource_type="publication-article",
    ),
    ("GCAM 8s", "h"): ReferenceInfo(
        short_name="GCAM integrated assessment modelling team, 2026 (in-prep)",
        licence="Paper, NA",
        reference=("GCAM team, ...: " "High scenario for CMIP7 ScenarioMIP, " "in-prep, 2026."),
        url="https://zenodo.org/records/18497404",
        resource_type="publication-article",
    ),
}
