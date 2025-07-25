"""
Reusable constants
"""

from __future__ import annotations

import openscm_units

Q = openscm_units.unit_registry.Quantity

GHG_LIFETIMES = {
    "cf4": Q(50000.0, "yr"),
    "c2f6": Q(10000.0, "yr"),
    "c3f8": Q(2600.0, "yr"),
    "c4f10": Q(2600.0, "yr"),
    "c5f12": Q(4100.0, "yr"),
    "c6f14": Q(3100.0, "yr"),
    "c7f16": Q(3000.0, "yr"),
    "c8f18": Q(3000.0, "yr"),
    "ch2cl2": Q(176.0 / 365.0, "yr"),
    "ch3br": Q(0.8, "yr"),
    "ch3cl": Q(0.9, "yr"),
    "chcl3": Q(178.0 / 365.0, "yr"),
    "hfc23": Q(228.0, "yr"),
    "hfc32": Q(5.27, "yr"),
    "hfc125": Q(30.7, "yr"),
    "hfc134a": Q(13.5, "yr"),
    "hfc143a": Q(51.8, "yr"),
    "hfc152a": Q(1.5, "yr"),
    "hfc227ea": Q(35.8, "yr"),
    "hfc236fa": Q(213.0, "yr"),
    "hfc245fa": Q(6.61, "yr"),
    "hfc365mfc": Q(8.86, "yr"),
    "hfc4310mee": Q(17.0, "yr"),
    "nf3": Q(569.0, "yr"),
    "sf6": Q((850 + 1280) / 2.0, "yr"),
    "so2f2": Q(36.0, "yr"),
    "cc4f8": Q(3200.0, "yr"),
    "n2o": Q(109.0, "yr"),
    "halon1201": Q(4.85, "yr"),
    "halon1202": Q(2.5, "yr"),
    "halon1211": Q(16, "yr"),
    "halon1301": Q(72, "yr"),
    "halon2402": Q(28, "yr"),
    "hcfc22": Q(11.6, "yr"),
    "hcfc141b": Q(8.81, "yr"),
    "hcfc142b": Q(17.1, "yr"),
    "cfc11": Q(52, "yr"),
    "cfc12": Q(102, "yr"),
    "cfc113": Q(93, "yr"),
    "cfc114": Q(189, "yr"),
    "cfc115": Q(540, "yr"),
    "ch3ccl3": Q(5, "yr"),
    "ccl4": Q(30, "yr"),
}
"""
Lifetimes for GHGs that have an approximately defined lifetime

Table A-5 of WMO 2022
https://csl.noaa.gov/assessments/ozone/2022/downloads/Annex_2022OzoneAssessment.pdf
"""

GHG_MOLECULAR_MASSES = {
    # Gas: molecular mass
    "cf4": Q(12.01 + 4 * 19.0, "gCF4 / mole"),
    "c2f6": Q(2 * 12.01 + 6 * 19.0, "gC2F6 / mole"),
    "c3f8": Q(3 * 12.01 + 8 * 19.0, "gC3F8 / mole"),
    "c4f10": Q(4 * 12.01 + 10 * 19.0, "gC4F10 / mole"),
    "c5f12": Q(5 * 12.01 + 12 * 19.0, "gC5F12 / mole"),
    "c6f14": Q(6 * 12.01 + 14 * 19.0, "gC6F14 / mole"),
    "c7f16": Q(7 * 12.01 + 16 * 19.0, "gC7F16 / mole"),
    "c8f18": Q(8 * 12.01 + 18 * 19.0, "gC8F18 / mole"),
    "ch2cl2": Q(12.01 + 2 * 1.0 + 2 * 35.45, "gCH2Cl2 / mole"),
    "ch3br": Q(12.01 + 3 * 1.0 + 79.90, "gCH3Br / mole"),
    "ch3cl": Q(12.01 + 3 * 1.0 + 35.45, "gCH3Cl / mole"),
    "chcl3": Q(12.01 + 1.0 + 3 * 35.45, "gCHCl3 / mole"),
    "hfc23": Q(12.01 + 1.0 + 19.0, "gHFC23 / mole"),  # CHF3
    "hfc32": Q(12.01 + 2 * 1.0 + 2 * 19.0, "gHFC32 / mole"),  # CH2F2
    "hfc125": Q(12.01 + 2 * 1.0 + 2 * 19.0 + 12.01 + 3 * 19.0, "gHFC125 / mole"),  # CHF2CF3
    "hfc134a": Q(12.01 + 2 * 1.0 + 19.0 + 12.01 + 3 * 19.0, "gHFC134a / mole"),  # CH2FCF3
    "hfc143a": Q(12.01 + 3 * 1.0 + 12.01 + 3 * 19.0, "gHFC143a / mole"),  # CH3CF3
    "hfc152a": Q(12.01 + 3 * 1.0 + 12.01 + 1.0 + 2 * 19.0, "gHFC152a / mole"),  # CH3CHF2
    "hfc227ea": Q(12.01 + 3 * 19.0 + 12.01 + 1.0 + 19.0 + 12.01 + 3 * 19.0, "gHFC227ea / mole"),  # CF3CHFCF3
    "hfc236fa": Q(12.01 + 3 * 19.0 + 12.01 + 2 * 1.0 + 12.01 + 3 * 19.0, "gHFC236fa / mole"),  # CF3CH2CF3
    "hfc245fa": Q(
        12.01 + 2 * 1.0 + 19.0 + 12.01 + 2 * 19.0 + 12.01 + 1.0 + 2 * 19.0, "gHFC245fa / mole"
    ),  # CH2FCF2CHF2
    "hfc365mfc": Q(
        12.01 + 3 * 1.0 + 12.01 + 2 * 19.0 + 12.01 + 2 * 1.0 + 12.01 + 3 * 19.0, "gHFC365mfc / mole"
    ),  # CH3CF2CH2CF3
    "hfc4310mee": Q(
        12.01 + 3 * 19.0 + 2 * (12.01 + 1.0 + 19.0) + 12.01 + 2 * 19.0 + 12.01 + 3 * 19.0, "gHFC4310 / mole"
    ),  # CF3CHFCHFCF2CF3
    "nf3": Q(14.01 + 3 * 19.0, "gNF3 / mole"),
    "sf6": Q(32.07 + 6 * 19.0, "gSF6 / mole"),
    "so2f2": Q(32.07 + 2 * 16.0 + 2 * 19.0, "gSO2F2 / mole"),
    "cc4f8": Q(4 * 12.01 + 8 * 19.0, "gcC4F8 / mole"),
    "n2o": Q(2 * 14.01 + 16.0, "gN2O / mole"),
    "halon1201": Q(12.01 + 1.0 + 79.9 + 2 * 19.0, "gHalon1201 / mole"),  # CHBrF2
    "halon1202": Q(12.01 + 2 * 79.9 + 2 * 19.0, "gHalon1202 / mole"),  # CBr2F2
    "halon1211": Q(12.01 + 79.9 + 35.45 + 2 * 19.0, "gHalon1211 / mole"),  # CBrClF2
    "halon1301": Q(12.01 + 79.9 + 3 * 19.0, "gHalon1301 / mole"),  # CBrF3
    "halon2402": Q(12.01 + 79.9 + 2 * 19.0 + 12.01 + 79.9 + 2 * 19.0, "gHalon2402 / mole"),  # CBrF2CBrF2
    "hcfc22": Q(12.01 + 1.0 + 2 * 19.0 + 35.45, "gHCFC22 / mole"),  # CHF2Cl
    "hcfc141b": Q(12.01 + 3 * 1.0 + 12.01 + 2 * 35.45 + 19.0, "gHCFC141b / mole"),  # CH3CCl2F
    "hcfc142b": Q(12.01 + 3 * 1.0 + 12.01 + 35.45 + 2 * 19.0, "gHCFC142b / mole"),  # CH3CClF2
    "cfc11": Q(12.01 + 3 * 35.45 + 19.0, "gCFC11 / mole"),  # CCl3F
    "cfc12": Q(12.01 + 2 * 35.45 + 2 * 19.0, "gCFC12 / mole"),  # CCl2F2
    "cfc113": Q(12.01 + 2 * 35.45 + 19.0 + 12.01 + 35.45 + 2 * 19.0, "gCFC113 / mole"),  # CCl2FCClF2
    "cfc114": Q(12.01 + 35.45 + 2 * 19.0 + 12.01 + 35.45 + 2 * 19.0, "gCFC114 / mole"),  # CClF2CClF2
    "cfc115": Q(12.01 + 35.45 + 2 * 19.0 + 12.01 + 3 * 19.0, "gCFC115 / mole"),  # CClF2CF3
    "ch3ccl3": Q(12.01 + 3 * 1.0 + 12.01 + 3 * 35.45, "gCH3CCl3 / mole"),
    "ccl4": Q(12.01 + 4 * 35.45, "gCCl4 / mole"),
}
"""
Molecular masses
"""

GHG_RADIATIVE_EFFICIENCIES = {
    # Chlorofluorocarbons
    "cfc11": Q(0.291, "W / m^2 / ppb"),
    "cfc11eq": Q(0.291, "W / m^2 / ppb"),
    "cfc12": Q(0.358, "W / m^2 / ppb"),
    "cfc12eq": Q(0.358, "W / m^2 / ppb"),
    "cfc113": Q(0.301, "W / m^2 / ppb"),
    "cfc114": Q(0.314, "W / m^2 / ppb"),
    "cfc115": Q(0.246, "W / m^2 / ppb"),
    # Hydrofluorochlorocarbons
    "hcfc22": Q(0.214, "W / m^2 / ppb"),
    "hcfc141b": Q(0.161, "W / m^2 / ppb"),
    "hcfc142b": Q(0.193, "W / m^2 / ppb"),
    # Hydrofluorocarbons
    "hfc23": Q(0.191, "W / m^2 / ppb"),
    "hfc32": Q(0.111, "W / m^2 / ppb"),
    "hfc125": Q(0.234, "W / m^2 / ppb"),
    "hfc134a": Q(0.167, "W / m^2 / ppb"),
    "hfc134aeq": Q(0.167, "W / m^2 / ppb"),
    "hfc143a": Q(0.168, "W / m^2 / ppb"),
    "hfc152a": Q(0.102, "W / m^2 / ppb"),
    "hfc227ea": Q(0.273, "W / m^2 / ppb"),
    "hfc236fa": Q(0.251, "W / m^2 / ppb"),
    "hfc245fa": Q(0.245, "W / m^2 / ppb"),
    "hfc365mfc": Q(0.228, "W / m^2 / ppb"),
    "hfc4310mee": Q(0.357, "W / m^2 / ppb"),
    # Chlorocarbons and Hydrochlorocarbons
    "ch3ccl3": Q(0.065, "W / m^2 / ppb"),
    "ccl4": Q(0.166, "W / m^2 / ppb"),
    "ch3cl": Q(0.005, "W / m^2 / ppb"),
    "ch2cl2": Q(0.029, "W / m^2 / ppb"),
    "chcl3": Q(0.074, "W / m^2 / ppb"),
    # Bromocarbons, Hydrobromocarbons and Halons
    "ch3br": Q(0.004, "W / m^2 / ppb"),
    "halon1201": Q(0.152, "W / m^2 / ppb"),
    "halon1211": Q(0.300, "W / m^2 / ppb"),
    "halon1301": Q(0.299, "W / m^2 / ppb"),
    "halon2402": Q(0.312, "W / m^2 / ppb"),
    # Fully Fluorinated Species
    "nf3": Q(0.204, "W / m^2 / ppb"),
    "sf6": Q(0.567, "W / m^2 / ppb"),
    "so2f2": Q(0.211, "W / m^2 / ppb"),
    "cf4": Q(0.099, "W / m^2 / ppb"),
    "c2f6": Q(0.261, "W / m^2 / ppb"),
    "c3f8": Q(0.270, "W / m^2 / ppb"),
    "cc4f8": Q(0.314, "W / m^2 / ppb"),
    "c4f10": Q(0.369, "W / m^2 / ppb"),
    "c5f12": Q(0.408, "W / m^2 / ppb"),
    "c6f14": Q(0.449, "W / m^2 / ppb"),
    "c7f16": Q(0.503, "W / m^2 / ppb"),
    "c8f18": Q(0.558, "W / m^2 / ppb"),
}
"""
Radiative efficiencies for GHGs

Linear hence approximate, but fine.

From Table 7.SM.6 of
https://www.ipcc.ch/report/ar6/wg1/downloads/report/IPCC_AR6_WGI_Chapter07_SM.pdf
"""

VARIABLE_TO_STANDARD_NAME_RENAMING = {
    "co2": "mole_fraction_of_carbon_dioxide_in_air",
    "ch4": "mole_fraction_of_methane_in_air",
    "n2o": "mole_fraction_of_nitrous_oxide_in_air",
    "c2f6": "mole_fraction_of_pfc116_in_air",
    "c3f8": "mole_fraction_of_pfc218_in_air",
    "c4f10": "mole_fraction_of_pfc3110_in_air",
    "c5f12": "mole_fraction_of_pfc4112_in_air",
    "c6f14": "mole_fraction_of_pfc5114_in_air",
    "c7f16": "mole_fraction_of_pfc6116_in_air",
    "c8f18": "mole_fraction_of_pfc7118_in_air",
    "cc4f8": "mole_fraction_of_pfc318_in_air",
    "ccl4": "mole_fraction_of_carbon_tetrachloride_in_air",
    "cf4": "mole_fraction_of_carbon_tetrafluoride_in_air",
    "cfc11": "mole_fraction_of_cfc11_in_air",
    "cfc113": "mole_fraction_of_cfc113_in_air",
    "cfc114": "mole_fraction_of_cfc114_in_air",
    "cfc115": "mole_fraction_of_cfc115_in_air",
    "cfc12": "mole_fraction_of_cfc12_in_air",
    "ch2cl2": "mole_fraction_of_dichloromethane_in_air",
    "ch3br": "mole_fraction_of_methyl_bromide_in_air",
    "ch3ccl3": "mole_fraction_of_hcc140a_in_air",
    "ch3cl": "mole_fraction_of_methyl_chloride_in_air",
    "chcl3": "mole_fraction_of_chloroform_in_air",
    "halon1211": "mole_fraction_of_halon1211_in_air",
    "halon1301": "mole_fraction_of_halon1301_in_air",
    "halon2402": "mole_fraction_of_halon2402_in_air",
    "hcfc141b": "mole_fraction_of_hcfc141b_in_air",
    "hcfc142b": "mole_fraction_of_hcfc142b_in_air",
    "hcfc22": "mole_fraction_of_hcfc22_in_air",
    "hfc125": "mole_fraction_of_hfc125_in_air",
    "hfc134a": "mole_fraction_of_hfc134a_in_air",
    "hfc143a": "mole_fraction_of_hfc143a_in_air",
    "hfc152a": "mole_fraction_of_hfc152a_in_air",
    "hfc227ea": "mole_fraction_of_hfc227ea_in_air",
    "hfc23": "mole_fraction_of_hfc23_in_air",
    "hfc236fa": "mole_fraction_of_hfc236fa_in_air",
    "hfc245fa": "mole_fraction_of_hfc245fa_in_air",
    "hfc32": "mole_fraction_of_hfc32_in_air",
    "hfc365mfc": "mole_fraction_of_hfc365mfc_in_air",
    "hfc4310mee": "mole_fraction_of_hfc4310mee_in_air",
    "nf3": "mole_fraction_of_nitrogen_trifluoride_in_air",
    "sf6": "mole_fraction_of_sulfur_hexafluoride_in_air",
    "so2f2": "mole_fraction_of_sulfuryl_fluoride_in_air",
    "cfc11eq": "mole_fraction_of_cfc11_eq_in_air",
    "cfc12eq": "mole_fraction_of_cfc12_eq_in_air",
    "hfc134aeq": "mole_fraction_of_hfc134a_eq_in_air",
}
"""Renaming from variable names to standard names"""


EQUIVALENT_SPECIES_COMPONENTS: dict[str, tuple[str, ...]] = {
    "cfc11eq": (
        "c2f6",
        "c3f8",
        "c4f10",
        "c5f12",
        "c6f14",
        "c7f16",
        "c8f18",
        "cc4f8",
        "ccl4",
        "cf4",
        "cfc11",
        "cfc113",
        "cfc114",
        "cfc115",
        "ch2cl2",
        "ch3br",
        "ch3ccl3",
        "ch3cl",
        "chcl3",
        "halon1211",
        "halon1301",
        "halon2402",
        "hcfc141b",
        "hcfc142b",
        "hcfc22",
        "hfc125",
        "hfc134a",
        "hfc143a",
        "hfc152a",
        "hfc227ea",
        "hfc23",
        "hfc236fa",
        "hfc245fa",
        "hfc32",
        "hfc365mfc",
        "hfc4310mee",
        "nf3",
        "sf6",
        "so2f2",
    ),
    "cfc12eq": (
        "cfc11",
        "cfc113",
        "cfc114",
        "cfc115",
        "cfc12",
        "ccl4",
        "ch2cl2",
        "ch3br",
        "ch3ccl3",
        "ch3cl",
        "chcl3",
        "halon1211",
        "halon1301",
        "halon2402",
        "hcfc141b",
        "hcfc142b",
        "hcfc22",
    ),
    "hfc134aeq": (
        "c2f6",
        "c3f8",
        "c4f10",
        "c5f12",
        "c6f14",
        "c7f16",
        "c8f18",
        "cc4f8",
        "cf4",
        "hfc125",
        "hfc134a",
        "hfc143a",
        "hfc152a",
        "hfc227ea",
        "hfc23",
        "hfc236fa",
        "hfc245fa",
        "hfc32",
        "hfc365mfc",
        "hfc4310mee",
        "nf3",
        "sf6",
        "so2f2",
    ),
}
"""
GHGs that contribute to each equivalent species
"""
