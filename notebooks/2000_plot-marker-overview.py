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
# # Plot marker overview

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Imports

# %% editable=true slideshow={"slide_type": ""}
from pathlib import Path

import pandas as pd
import pandas_indexing as pix
import pandas_openscm
import pandas_openscm.db

from cmip7_scenariomip_ghg_generation.scenario_info import ScenarioInfo

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Parameters

# %% editable=true slideshow={"slide_type": ""} tags=["parameters"]
scenario_info_markers: str = (
    "REMIND-MAgPIE 3.5-4.10;SSP1 - Very Low Emissions;vllo;;MESSAGEix-GLOBIOM-GAINS 2.1-M-R12;SSP2 - Low Emissions;l"
)
emissions_complete_dir: str = "../output-bundles/dev-test/data/interim/complete-emissions"
magicc_output_db_dir: str = "../output-bundles/dev-test/data/interim/magicc-output/db"
magicc_db_backend_str: str = "feather"


# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Parse parameters

# %% editable=true slideshow={"slide_type": ""}
siml = []
for v in scenario_info_markers.split(";;"):
    model, scenario, cmip_scenario_name = v.split(";")
    siml.append(
        ScenarioInfo(
            cmip_scenario_name=cmip_scenario_name,
            model=model,
            scenario=scenario,
        )
    )

scenario_info_markers_p = tuple(siml)
emissions_complete_dir_p = Path(emissions_complete_dir)
magicc_output_db = pandas_openscm.db.OpenSCMDB(
    backend_data=pandas_openscm.db.DATA_BACKENDS.get_instance(magicc_db_backend_str),
    backend_index=pandas_openscm.db.INDEX_BACKENDS.get_instance(magicc_db_backend_str),
    db_dir=Path(magicc_output_db_dir),
)
scenario_info_markers_p

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Set up

# %% editable=true slideshow={"slide_type": ""}
pandas_openscm.register_pandas_accessor()

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Load data

# %%
emissions = pix.concat(
    [pd.read_feather(emissions_complete_dir_p / f"{si.to_file_stem()}.feather") for si in scenario_info_markers_p]
)

# emissions

# %%
magiccc_output_l = []
for si in scenario_info_markers_p:
    magiccc_output_l.append(
        magicc_output_db.load(
            pix.isin(
                model=si.model,
                scenario=si.scenario,
                climate_model="MAGICCv7.6.0a3",
            )
            & pix.ismatch(variable=["Surface Air Temperature Change", "Effective Radiative Forcing**"]),
            progress=True,
        )
    )

magiccc_output = pix.concat(magiccc_output_l)
# magiccc_output

# %% [markdown]
# ## Add useful metadata to data

# %%
cmip_scenario_name_d = {(si.model, si.scenario): si.cmip_scenario_name for si in scenario_info_markers_p}
cmip_scenario_name_d


# %%
def add_cmip_scenario_name(indf: pd.DataFrame) -> pd.DataFrame:
    """Add CMIP scenario name to the index"""
    cmip_scenario_names = indf.index.droplevel(indf.index.names.difference(["model", "scenario"])).map(
        cmip_scenario_name_d
    )
    res = indf.openscm.set_index_levels({"cmip_scenario_name": cmip_scenario_names})

    return res


# %%
emissions_pdf = add_cmip_scenario_name(emissions)
# emissions_pdf

# %%
magiccc_output_pdf = add_cmip_scenario_name(magiccc_output)
# magiccc_output_pdf

# %% [markdown]
# ## Plot

# %%
assert False
