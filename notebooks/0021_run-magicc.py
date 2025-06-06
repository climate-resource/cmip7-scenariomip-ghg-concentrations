# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.17.2
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown] editable=true slideshow={"slide_type": ""}
# # Run MAGICC

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Imports

# %% editable=true slideshow={"slide_type": ""}
import json
import os
import platform
from functools import partial
from pathlib import Path
from typing import Any

import pandas as pd
import pandas_indexing as pix
import pandas_openscm
from gcages.renaming import SupportedNamingConventions, convert_variable_name
from gcages.scm_running import convert_openscm_runner_output_names_to_magicc_output_names, run_scms

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Parameters

# %% editable=true slideshow={"slide_type": ""} tags=["parameters"]
model: str = "WITCH 6.0"
scenario: str = "SSP1 - Very Low Emissions"
complete_file: str = (
    "../output-bundles/dev-test/data/interim/complete-emissions/SSP1_-_Very_Low_Emissions_WITCH_6-0.feather"
)
magicc_version: str = "MAGICCv7.6.0a3"
magicc_exe: str = "../magicc/magicc-v7.6.0a3/bin/magicc-darwin-arm64"
magicc_prob_distribution: str = "../magicc/magicc-v7.6.0a3/configs/magicc-ar7-fast-track-drawnset-v0-3-0.json"
n_magicc_workers: int = 4
out_file: str = (
    "../output-bundles/dev-test/data/interim/magicc-output/SSP1_-_Very_Low_Emissions_WITCH_6-0_magicc-results.feather"
)


# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Parse parameters

# %% editable=true slideshow={"slide_type": ""}
complete_file_p = Path(complete_file)
magicc_exe_p = Path(magicc_exe)
magicc_prob_distribution_p = Path(magicc_prob_distribution)
out_file_p = Path(out_file)

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Set up

# %% editable=true slideshow={"slide_type": ""}
pandas_openscm.register_pandas_accessor()

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Load data

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ### Complete scenario

# %%
complete = pd.read_feather(complete_file_p)
# complete

# %% [markdown]
# ## Run MAGICC

# %% [markdown]
# ### Convert scenario to OpenSCM-Runner names

# %%
complete_openscm_runner = complete.openscm.update_index_levels(
    {
        "variable": partial(
            convert_variable_name,
            from_convention=SupportedNamingConventions.GCAGES,
            to_convention=SupportedNamingConventions.OPENSCM_RUNNER,
        )
    },
)
# complete_openscm_runner

# %% [markdown]
# ### Configure

# %%
os.environ["MAGICC_EXECUTABLE_7"] = str(magicc_exe_p)

# %%
MAGICC_START_SCENARIO_YEAR = 2015

# %%
complete_for_magicc = complete.loc[:, MAGICC_START_SCENARIO_YEAR:]
# complete_for_magicc

# %%
output_variables = (
    # GSAT
    "Surface Air Temperature Change",
    # # GMST
    "Surface Air Ocean Blended Temperature Change",
    # # ERFs
    "Effective Radiative Forcing",
    "Effective Radiative Forcing|Anthropogenic",
    "Effective Radiative Forcing|Aerosols",
    "Effective Radiative Forcing|Aerosols|Direct Effect",
    "Effective Radiative Forcing|Aerosols|Direct Effect|BC",
    "Effective Radiative Forcing|Aerosols|Direct Effect|OC",
    "Effective Radiative Forcing|Aerosols|Direct Effect|SOx",
    "Effective Radiative Forcing|Aerosols|Indirect Effect",
    "Effective Radiative Forcing|Greenhouse Gases",
    "Effective Radiative Forcing|CO2",
    "Effective Radiative Forcing|CH4",
    "Effective Radiative Forcing|N2O",
    "Effective Radiative Forcing|F-Gases",
    "Effective Radiative Forcing|Montreal Protocol Halogen Gases",
    "Effective Radiative Forcing|Ozone",
    "Effective Radiative Forcing|Tropospheric Ozone",
    "Effective Radiative Forcing|Stratospheric Ozone",
    "Effective Radiative Forcing|Solar",
    "Effective Radiative Forcing|Volcanic",
    # # Heat uptake
    "Heat Uptake",
    "Heat Uptake|Ocean",
    # # Atmospheric concentrations
    "Atmospheric Concentrations|CO2",
    "Atmospheric Concentrations|CH4",
    "Atmospheric Concentrations|N2O",
    "Atmospheric Concentrations|C2F6",
    "Atmospheric Concentrations|C3F8",
    "Atmospheric Concentrations|C4F10",
    "Atmospheric Concentrations|C5F12",
    "Atmospheric Concentrations|C6F14",
    "Atmospheric Concentrations|C7F16",
    "Atmospheric Concentrations|C8F18",
    "Atmospheric Concentrations|cC4F8",
    "Atmospheric Concentrations|CF4",
    "Atmospheric Concentrations|CH2Cl2",
    "Atmospheric Concentrations|CHCl3",
    "Atmospheric Concentrations|HFC125",
    "Atmospheric Concentrations|HFC134a",
    "Atmospheric Concentrations|HFC143a",
    "Atmospheric Concentrations|HFC152a",
    "Atmospheric Concentrations|HFC227ea",
    "Atmospheric Concentrations|HFC23",
    "Atmospheric Concentrations|HFC236fa",
    "Atmospheric Concentrations|HFC245fa",
    "Atmospheric Concentrations|HFC32",
    "Atmospheric Concentrations|HFC365mfc",
    "Atmospheric Concentrations|HFC4310mee",
    "Atmospheric Concentrations|NF3",
    "Atmospheric Concentrations|SF6",
    "Atmospheric Concentrations|SO2F2",
    ## Prescribed conc gases for comparison
    "Atmospheric Concentrations|CCl4",
    "Atmospheric Concentrations|CFC11",
    "Atmospheric Concentrations|CFC12",
    "Atmospheric Concentrations|CFC113",
    "Atmospheric Concentrations|CFC114",
    "Atmospheric Concentrations|CFC115",
    "Atmospheric Concentrations|CH3Br",
    "Atmospheric Concentrations|CH3CCl3",
    "Atmospheric Concentrations|CH3Cl",
    "Atmospheric Concentrations|Halon1202",
    "Atmospheric Concentrations|Halon1211",
    "Atmospheric Concentrations|Halon1301",
    "Atmospheric Concentrations|Halon2402",
    "Atmospheric Concentrations|HCFC141b",
    "Atmospheric Concentrations|HCFC142b",
    "Atmospheric Concentrations|HCFC22",
    # # Carbon cycle
    # "Net Atmosphere to Land Flux|CO2",
    # "Net Atmosphere to Ocean Flux|CO2",
    # "CO2_CURRENT_NPP",
    # # Permafrost
    # "Net Land to Atmosphere Flux|CO2|Earth System Feedbacks|Permafrost",
    # "Net Land to Atmosphere Flux|CH4|Earth System Feedbacks|Permafrost",
    "Sea Level Rise",
)


# %%
def load_magicc_cfgs(
    prob_distribution_path: Path,
    output_variables: tuple[str, ...],
    startyear: int = 1750,
) -> dict[str, list[dict[str, Any]]]:
    """
    Load MAGICC's configuration

    Parameters
    ----------
    prob_distribution_path
        Path to the file containing the probabilistic distribution

    output_variables
        Output variables

    startyear
        Starting year of the runs

    Returns
    -------
    :
        Config that can be used to run MAGICC
    """
    with open(prob_distribution_path) as fh:
        cfgs_raw = json.load(fh)

    cfgs_physical = [
        {
            "run_id": c["paraset_id"],
            **{k.lower(): v for k, v in c["nml_allcfgs"].items()},
        }
        for c in cfgs_raw["configurations"]
    ]

    common_cfg = {
        "startyear": startyear,
        # Note: endyear handled in gcages, which I don't love but is fine for now
        "out_dynamic_vars": convert_openscm_runner_output_names_to_magicc_output_names(output_variables),
        "out_ascii_binary": "BINARY",
        "out_binary_format": 2,
    }

    run_config = [{**common_cfg, **physical_cfg} for physical_cfg in cfgs_physical]
    climate_models_cfgs = {"MAGICC7": run_config}

    return climate_models_cfgs


# %%
climate_models_cfgs = load_magicc_cfgs(
    prob_distribution_path=magicc_prob_distribution_p,
    output_variables=output_variables,
    startyear=1750,
)

# %%
# climate_models_cfgs["MAGICC7"][0]["out_dynamic_vars"]

# %%
if magicc_version == "MAGICCv7.5.3" and platform.system() == "Darwin" and platform.processor() == "arm":
    os.environ["DYLD_LIBRARY_PATH"] = "/opt/homebrew/opt/gfortran/lib/gcc/current/"

# %% [markdown]
# ### Run

# %%
res = run_scms(
    scenarios=complete_openscm_runner,
    climate_models_cfgs=climate_models_cfgs,
    output_variables=output_variables,
    scenario_group_levels=["model", "scenario"],
    n_processes=n_magicc_workers,
    db=None,
    verbose=True,
    progress=True,
)

res

# %% [markdown]
# ### Quick look at plots

# %%
res.loc[pix.isin(variable="Surface Air Temperature Change"), 2000:].openscm.plot_plume_after_calculating_quantiles(
    quantile_over="run_id"
)

# %%
erfs = res.loc[pix.ismatch(variable="Effective Radiative Forcing**"), 2000:].openscm.groupby_except("run_id").median()
ax = erfs.pix.project("variable").T.plot()
ax.legend(loc="center left", bbox_to_anchor=(1.05, 0.5))

# %%
# concs = res.loc[pix.ismatch(variable="Atmospheric Concentrations|*"), 2000:].openscm.groupby_except("run_id").median()
# sns.relplot(
#     data=concs.openscm.to_long_data(),
#     x="time",
#     y="value",
#     col="variable",
#     col_wrap=3,
#     facet_kws=dict(sharey=False),
# )

# %% [markdown]
# ## Save

# %%
out_file_p.parent.mkdir(exist_ok=True, parents=True)
res.to_feather(out_file_p)
