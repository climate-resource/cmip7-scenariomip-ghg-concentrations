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

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pandas_indexing as pix
import pandas_openscm
import pandas_openscm.db
import tqdm.auto

from cmip7_scenariomip_ghg_generation.scenario_info import ScenarioInfo

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Parameters

# %% editable=true slideshow={"slide_type": ""} tags=["parameters"]
scenario_info_markers: str = (
    "WITCH 6.0;SSP5 - Medium-Low Emissions_a;hl;;"
    "REMIND-MAgPIE 3.5-4.10;SSP1 - Very Low Emissions;vllo;;"
    "MESSAGEix-GLOBIOM-GAINS 2.1-M-R12;SSP2 - Low Emissions;l;;"
    "IMAGE 3.4;SSP2 - Medium Emissions;m;;"
    "GCAM 7.1 scenarioMIP;SSP3 - High Emissions;h;;"
    "AIM 3.0;SSP2 - Low Overshoot;vlho;;"
    "COFFEE 1.6;SSP2 - Medium-Low Emissions;ml"
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
# magicc_output_db.load_metadata().to_frame(index=False)

# %%
magiccc_output_l = []
for si in tqdm.auto.tqdm(scenario_info_markers_p):
    magiccc_output_l.append(
        magicc_output_db.load(
            pix.isin(
                model=si.model,
                scenario=si.scenario,
                climate_model="MAGICCv7.6.0a3",
            )
            & pix.ismatch(variable=["Surface Air Temperature Change", "Effective Radiative Forcing**"]),
            # progress=True,
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
palette = {
    "vllo": "#499edb",
    "vlho": "#4b3d89",
    "l": "#2e9e68",
    "ml": "#e1ad01",
    "m": "#f7a84f",
    "h": "#4c2525",
    "hl": "#7f3e3e",
}

# %% [markdown]
# ### Just temperatures

# %%
magiccc_output_pdf_q = (
    magiccc_output_pdf.openscm.groupby_except("run_id")
    .quantile([0.05, 0.17, 0.5, 0.83, 0.95])
    .openscm.fix_index_name_after_groupby_quantile()
)
# magiccc_output_pdf_q

# %%
pdf = magiccc_output_pdf_q.loc[pix.isin(variable="Surface Air Temperature Change"), 1950:]
quantiles_plumes = [(0.5, 0.95), ((0.05, 0.95), 0.3)]
quantiles_plumes = [(0.5, 0.95), ((0.17, 0.83), 0.5)]

fig, axes = plt.subplots(ncols=2, figsize=(10, 4))

for ax, xlim, yticks, ylim, show_legend in (
    (axes[0], (2015, 2100), np.arange(1.0, 2.51, 0.1), (1.0, 2.5), False),
    (axes[1], (1950, 2100), np.arange(0.5, 5.51, 0.5), None, True),
):
    pdf.loc[:, xlim[0] : xlim[1]].openscm.plot_plume(
        quantiles_plumes=quantiles_plumes,
        linewidth=3,
        hue_var="cmip_scenario_name",
        hue_var_label="Scenario",
        palette=palette,
        ax=ax,
    )
    ax.set_yticks(yticks)
    ax.grid()
    ax.set_xlim(xlim)

    if ylim is not None:
        ax.set_ylim(ylim)

    if not show_legend:
        ax.get_legend().remove()

# %%
pi_period = np.arange(1850, 1900 + 1)
assessment_period = np.arange(1995, 2014 + 1)
assessed_gsat = 0.85

tmp = magiccc_output_pdf.loc[pix.isin(variable="Surface Air Temperature Change"), :]

tmp_pi = tmp.loc[:, pi_period].mean(axis="columns")
tmp_rel_pi = tmp.subtract(tmp_pi, axis="rows")
gsat = (
    tmp_rel_pi.subtract(
        tmp_rel_pi.loc[:, assessment_period]
        .mean(axis="columns")
        .groupby(["climate_model", "model", "scenario"])
        .median(),
        axis="rows",
    )
    + assessed_gsat
)

gsat_q = (
    gsat.openscm.groupby_except("run_id")
    .quantile([0.05, 0.17, 0.33, 0.5, 0.67, 0.83, 0.95])
    .openscm.fix_index_name_after_groupby_quantile()
)
# gsat_q

# %%
pdf = gsat_q
quantiles_plumes = [(0.5, 0.95), ((0.05, 0.95), 0.3)]
quantiles_plumes = [(0.5, 0.95), ((0.17, 0.83), 0.5)]
quantiles_plumes = [(0.5, 0.95), ((0.33, 0.67), 0.5)]

# TODO: switch to mosaic
fig, axes = plt.subplots(nrows=2, ncols=2, figsize=(12, 10))

for ax, xlim, yticks, ylim, show_legend in (
    (axes[0][1], (1950, 2100), np.arange(0.5, 5.51, 0.5), None, True),
    (axes[1][0], (2015, 2100), np.arange(1.0, 2.51, 0.1), (1.0, 2.5), False),
    (axes[1][1], (2023, 2050), np.arange(1.0, 2.11, 0.1), (1.0, 2.2), False),
):
    pdf.loc[:, xlim[0] : xlim[1]].openscm.plot_plume(
        quantiles_plumes=quantiles_plumes,
        linewidth=3,
        hue_var="cmip_scenario_name",
        hue_var_label="Scenario",
        palette=palette,
        ax=ax,
    )
    ax.set_yticks(yticks)
    # ax.yaxis.tick_right()
    ax.tick_params(right=True, left=True, labelright=True, axis="y")
    ax.grid()
    ax.set_xlim(xlim)

    if ylim is not None:
        ax.set_ylim(ylim)

    if not show_legend:
        ax.get_legend().remove()

# TODO: fix legend position
# plt.tight_layout()

# %% [markdown]
# ### Overview

# %%
# Breakdown in reverse causal chain:
# - temperatures
# - ERFs and closest linked emissions

# %%
# Other notebooks:
# - comparison of MAGICC mode and concentration driven mode projections for markers
#   - i.e. make sure that the difference isn't too large
# - comparison of CMIP6 and CMIP7 ScenarioMIP global-mean annual-mean concentration
#   history and projections and impact of MAGICC update on concentrations
#   - i.e. breakdown change into scenario change and SCM change

# %%
assert False
