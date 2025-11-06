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
import pint
import seaborn as sns
import tqdm.auto

from cmip7_scenariomip_ghg_generation.scenario_info import ScenarioInfo

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Parameters

# %% editable=true slideshow={"slide_type": ""} tags=["parameters"]
scenario_info_markers: str = (
    "WITCH 6.0;SSP5 - Medium-Low Emissions_a;hl;;"
    "REMIND-MAgPIE 3.5-4.11;SSP1 - Very Low Emissions;vl;;"
    "MESSAGEix-GLOBIOM-GAINS 2.1-M-R12;SSP2 - Low Emissions;l;;"
    "IMAGE 3.4;SSP2 - Medium Emissions;m;;"
    "GCAM 7.1 scenarioMIP;SSP3 - High Emissions;h;;"
    "AIM 3.0;SSP2 - Low Overshoot;ln;;"
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

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Set up

# %% editable=true slideshow={"slide_type": ""}
pix.set_openscm_registry_as_default()
UR = pint.get_application_registry()
pandas_openscm.register_pandas_accessor()

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Load data

# %%
emissions_l = []
for si in scenario_info_markers_p:
    try:
        tmp = pd.read_feather(emissions_complete_dir_p / f"{si.to_file_stem()}.feather")
    except FileNotFoundError:
        print(f"No output for {si=}")
        continue

    emissions_l.append(tmp)

emissions = pix.concat(emissions_l)

# emissions

# %%
magiccc_output_l = []
for si in tqdm.auto.tqdm(scenario_info_markers_p):
    try:
        tmp = magicc_output_db.load(
            pix.isin(
                model=si.model,
                scenario=si.scenario,
                climate_model="MAGICCv7.6.0a3",
            )
            & pix.ismatch(variable=["Surface Air Temperature Change", "Effective Radiative Forcing**"]),
            # progress=True,
        )
    except ValueError:
        print(f"No output for {si=}")
        continue

    magiccc_output_l.append(tmp)

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
    "vl": "#24a4ff",
    "ln": "#4a0daf",
    "l": "#00cc69",
    "ml": "#f5ac00",
    "m": "#ffa9dc",
    "h": "#700000",
    "hl": "#8f003b",
}

scenario_order = ["vl", "ln", "l", "ml", "m", "hl", "h"]

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
).pix.assign(variable="GSAT assessed")

gsat_q = (
    gsat.openscm.groupby_except("run_id")
    .quantile([0.05, 0.17, 0.33, 0.5, 0.67, 0.83, 0.95])
    .openscm.fix_index_name_after_groupby_quantile()
)
# gsat_q

# %%
pdf = gsat_q.sort_index()

fig, axes = plt.subplot_mosaic(
    [
        ["full"],
        ["century"],
        ["mid-century"],
    ],
    figsize=(10, 12),
)


def create_legend(ax, handles) -> None:
    """Create legend for the plot"""
    handles_labels = [h.get_label() for h in handles]
    scenario_header_idx = handles_labels.index("Scenario")
    variable_header_idx = handles_labels.index("Variable")
    scenario_handles_d = {h.get_label(): h for h in handles[scenario_header_idx + 1 : variable_header_idx]}

    handles_scenarios_ordered = []
    for s in scenario_order:
        try:
            handles_scenarios_ordered.append(scenario_handles_d[s])
        except KeyError:
            continue

    handles_sorted = [
        *handles[: scenario_header_idx + 1],
        *handles_scenarios_ordered,
        *handles[variable_header_idx:],
    ]

    ax.legend(handles=handles_sorted, loc="center left", bbox_to_anchor=(1.05, 0.5))


for ax, xlim, yticks, ylim, show_legend, qps in (
    (axes["full"], (1950, 2100), np.arange(0.5, 5.01, 0.5), None, True, [(0.5, 0.95), ((0.05, 0.95), 0.2)]),
    (axes["century"], (2015, 2100), np.arange(1.0, 2.51, 0.1), (1.0, 2.5), True, [(0.5, 0.95), ((0.33, 0.67), 0.5)]),
    (axes["mid-century"], (2023, 2050), np.arange(1.3, 2.01, 0.1), (1.3, 2.0), True, [(0.5, 0.95)]),
):
    pdf.loc[:, xlim[0] : xlim[1]].openscm.plot_plume(
        quantiles_plumes=qps,
        linewidth=3,
        hue_var="cmip_scenario_name",
        hue_var_label="Scenario",
        palette=palette,
        ax=ax,
        create_legend=create_legend,
    )
    ax.set_yticks(yticks)
    # ax.yaxis.tick_right()
    ax.tick_params(right=True, left=True, labelright=True, axis="y")
    # ax.grid()
    ax.set_xlim(xlim)
    ax.set_xlabel("")

    if ylim is not None:
        ax.set_ylim(ylim)

    if not show_legend:
        ax.get_legend().remove()

# axes["mid-century"].tick_params(right=True, left=False, labelleft=False, labelright=True, axis="y")
for level in [1.5, 2.0]:
    axes["full"].axhline(level, linestyle="--", color="gray", zorder=1.1)

for level in [1.5, 1.7, 1.8, 2.0]:
    axes["century"].axhline(level, linestyle="--", color="gray", zorder=1.1)

axes["mid-century"].grid()
# TODO: fix legend position
# plt.tight_layout()

# %%
scenario_order_in_dataset = [v for v in scenario_order if v in pdf.index.get_level_values("cmip_scenario_name")]
scenario_order_in_dataset

# %%
pdf.index.droplevel(pdf.index.names.difference(["model", "scenario", "cmip_scenario_name"])).drop_duplicates().to_frame(
    index=False
).set_index("cmip_scenario_name").loc[scenario_order_in_dataset]

# %% [markdown]
# ### Overview
#
# Plot below gives a rough breakdown in the reverse of the causal chain.

# %%
# TODO: calculate extras in upstream notebook
emissions_pdf_incl_extras = pix.concat(
    [
        emissions_pdf,
        emissions_pdf.loc[pix.ismatch(variable="Emissions|CO2|*")]
        .openscm.groupby_except("variable")
        .sum()
        .pix.assign(variable="Emissions|CO2"),
    ]
)

gwp = "AR6GWP100"
with UR.context(gwp):
    ghg_eq = emissions_pdf.loc[
        ~pix.ismatch(variable=[f"Emissions|{s}" for s in ["SOx", "NOx", "BC", "OC", "CO", "NMVOC", "NH3"]])
    ].pix.convert_unit("GtCO2/yr")

emissions_pdf_incl_extras = pix.concat(
    [
        emissions_pdf_incl_extras,
        ghg_eq.openscm.groupby_except("variable").sum().pix.assign(variable=f"Emissions|GHG {gwp}"),
    ]
).pix.convert_unit({"Mt CO2/yr": "Gt CO2/yr"})

# emissions_pdf_incl_extras

# %%
xlim = (2015, 2100)
quantiles_plumes = [
    (0.5, 0.95),
    # ((0.05, 0.95), 0.2),
]

mosaic = [
    ["GSAT assessed", "Effective Radiative Forcing"],
    ["Effective Radiative Forcing|Greenhouse Gases", "Effective Radiative Forcing|Aerosols"],
    ["Emissions|GHG AR6GWP100", "."],
    ["Effective Radiative Forcing|CO2", "Emissions|CO2"],
    ["Emissions|CO2|Fossil", "Emissions|CO2|Biosphere"],
    ["Effective Radiative Forcing|CH4", "Emissions|CH4"],
    ["Effective Radiative Forcing|Ozone", "Emissions|NMVOC"],
    ["Emissions|CO", "."],
    ["Effective Radiative Forcing|N2O", "Emissions|N2O"],
    ["Effective Radiative Forcing|Aerosols|Direct Effect", "Effective Radiative Forcing|Aerosols|Indirect Effect"],
    ["Emissions|NOx", "Emissions|NH3"],
    ["Effective Radiative Forcing|Aerosols|Direct Effect|BC", "Emissions|BC"],
    ["Effective Radiative Forcing|Aerosols|Direct Effect|OC", "Emissions|OC"],
    ["Effective Radiative Forcing|Aerosols|Direct Effect|SOx", "Emissions|SOx"],
    ["Effective Radiative Forcing|Montreal Protocol Halogen Gases", "."],
]
fig, axes = plt.subplot_mosaic(
    mosaic,
    figsize=(12, 4 * len(mosaic)),
)

legend_variables = [v[1] for v in mosaic]
zero_line_variables = [v for vv in mosaic for v in vv if "CO2" in v or "GHG" in v]


def create_legend(ax, handles) -> None:
    """Create legend for the plot"""
    handles_labels = [h.get_label() for h in handles]
    scenario_header_idx = handles_labels.index("Scenario")
    variable_header_idx = handles_labels.index("Variable")
    scenario_handles_d = {h.get_label(): h for h in handles[scenario_header_idx + 1 : variable_header_idx]}

    handles_scenarios_ordered = []
    for s in scenario_order:
        try:
            handles_scenarios_ordered.append(scenario_handles_d[s])
        except KeyError:
            continue

    handles_sorted = [
        *handles[: scenario_header_idx + 1],
        *handles_scenarios_ordered,
        *handles[variable_header_idx:],
    ]

    ax.legend(handles=handles_sorted, loc="center left", bbox_to_anchor=(1.05, 0.5))


for variable, ax in tqdm.auto.tqdm(axes.items()):
    variable_locator = pix.isin(variable=variable)

    if variable in gsat_q.pix.unique("variable"):
        pdf = gsat_q.loc[variable_locator, xlim[0] : xlim[1]]
        pdf.openscm.plot_plume(
            quantiles_plumes=quantiles_plumes,
            linewidth=3,
            hue_var="cmip_scenario_name",
            hue_var_label="Scenario",
            palette=palette,
            ax=ax,
            create_legend=create_legend,
        )

    elif variable in magiccc_output_pdf_q.pix.unique("variable"):
        pdf = magiccc_output_pdf_q.loc[variable_locator, xlim[0] : xlim[1]]
        pdf.openscm.plot_plume(
            quantiles_plumes=quantiles_plumes,
            linewidth=3,
            hue_var="cmip_scenario_name",
            hue_var_label="Scenario",
            palette=palette,
            ax=ax,
            create_legend=create_legend,
        )

    elif variable in emissions_pdf_incl_extras.pix.unique("variable"):
        vdf = emissions_pdf_incl_extras.loc[variable_locator, :]
        pdf = vdf.loc[:, xlim[0] : xlim[1]]
        sns.lineplot(
            data=pdf.openscm.to_long_data(),
            x="time",
            y="value",
            hue="cmip_scenario_name",
            hue_order=scenario_order,
            palette=palette,
            linewidth=3,
            ax=ax,
        )

        ax.axhline(vdf.loc[:, 1750].iloc[0], linestyle=":", color="black")

    ax.set_title(variable)
    if variable in legend_variables:
        sns.move_legend(ax, loc="center left", bbox_to_anchor=(1.05, 0.5))

    else:
        ax.legend().remove()

    if variable in zero_line_variables:
        ax.axhline(0.0, linestyle="--", color="gray", zorder=1.1)

    elif variable.startswith("Emissions"):
        ax.set_ylim(ymin=0.0)

    ax.set_xlabel("")

    unit_l = pdf.pix.unique("unit")
    if len(unit_l) != 1:
        raise AssertionError(unit_l)

    unit = unit_l[0]
    ax.set_ylabel(unit)
    ax.set_xlim(xlim)

# %%
# Other notebooks to write:
# - comparison of MAGICC mode and concentration driven mode projections for markers
#   - i.e. make sure that the difference isn't too large
# - comparison of CMIP6 and CMIP7 ScenarioMIP global-mean annual-mean concentration
#   history and projections and impact of MAGICC update on concentrations
#   - i.e. breakdown change into scenario change and SCM change
