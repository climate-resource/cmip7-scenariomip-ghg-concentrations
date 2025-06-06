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
# # Create a complete emissions scenario
#
# 1. convert to gcages names
# 1. interpolate to annual
# 1. do scaling-based infilling
# 1. join with history

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Imports

# %% editable=true slideshow={"slide_type": ""}
from functools import partial
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import openscm_units
import pandas as pd
import pandas_indexing as pix
import pandas_openscm
import seaborn as sns
from gcages.renaming import SupportedNamingConventions, convert_variable_name
from pandas_openscm.io import load_timeseries_csv

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Parameters

# %% editable=true slideshow={"slide_type": ""} tags=["parameters"]
model: str = "MESSAGEix-GLOBIOM-GAINS 2.1-M-R12"
scenario: str = "SSP2 - Low Emissions"
scenario_file: str = "../output-bundles/dev-test/data/interim/input-emissions/0009-zn_0003_0003_0002/SSP2_-_Low_Emissions_MESSAGEix-GLOBIOM-GAINS_2-1-M-R12.feather"  # noqa: E501
inverse_emissions_file: str = "../output-bundles/dev-test/data/processed/inverse-emissions/single-concentration-projection_inverse-emissions.feather"  # noqa: E501
history_file: str = "../output-bundles/dev-test/data/interim/input-emissions/0009-zn_0003_0003_0002/historical.feather"
out_file: str = "../output-bundles/dev-test/data/processed/complete-emissions/SSP2_-_Low_Emissions_MESSAGEix-GLOBIOM-GAINS_2-1-M-R12.csv"  # noqa: E501


# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Parse parameters

# %% editable=true slideshow={"slide_type": ""}
scenario_file_p = Path(scenario_file)
inverse_emissions_file_p = Path(inverse_emissions_file)
history_file_p = Path(history_file)
out_file_p = Path(out_file)

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Set up

# %% editable=true slideshow={"slide_type": ""}
pandas_openscm.register_pandas_accessor()
pix.set_openscm_registry_as_default()
Q = openscm_units.unit_registry.Quantity

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Load data

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ### History

# %%
ltc = partial(
    load_timeseries_csv,
    index_columns=["model", "scenario", "region", "variable", "unit"],
    out_columns_type=int,
    out_columns_name="year",
)

# %%
history = pd.read_feather(history_file_p)
# history

# %% [markdown]
# ### Inverse emissions

# %%
inverse_emissions = pd.read_feather(inverse_emissions_file_p)
# inverse_emissions

# %% [markdown]
# ### Scenario without infilling

# %% editable=true slideshow={"slide_type": ""}
raw_scenario = pd.read_feather(scenario_file_p)
# raw_scenario

# %% [markdown]
# ## Convert to gcages names

# %%
gcages_scenario = raw_scenario.openscm.update_index_levels(
    {
        "variable": partial(
            convert_variable_name,
            from_convention=SupportedNamingConventions.CMIP7_SCENARIOMIP,
            to_convention=SupportedNamingConventions.GCAGES,
        )
    }
)
# gcages_scenario

# %%
gcages_history = history.openscm.update_index_levels(
    {
        "variable": partial(
            convert_variable_name,
            from_convention=SupportedNamingConventions.CMIP7_SCENARIOMIP,
            to_convention=SupportedNamingConventions.GCAGES,
        )
    }
)
# gcages_history

# %% [markdown]
# ## Interpolate to annual

# %%
annual_scenario = gcages_scenario.copy()
missing_years = np.setdiff1d(np.arange(raw_scenario.columns.min(), raw_scenario.columns.max()), raw_scenario.columns)
annual_scenario[missing_years] = np.nan
annual_scenario = annual_scenario.sort_index(axis="columns")
annual_scenario = annual_scenario.T.interpolate(method="index").T

pdf = pix.concat(
    [gcages_scenario.pix.assign(stage="raw"), annual_scenario.pix.assign(stage="annual")]
).openscm.to_long_data()
sns.relplot(
    data=pdf,
    x="time",
    y="value",
    kind="scatter",
    col="variable",
    col_wrap=3,
    hue="stage",
    style="stage",
    facet_kws=dict(sharey=False),
    alpha=0.5,
)

# %% [markdown]
# ## Scaling-based infilling
#
# For some species, we infill by simply assuming a scaling following another emissions's trajectory.
#
# For each follower, leader pair we need:
# - f_harmonisation_year: The value of the follower in the harmonisation year
# - l_harmonisation_year: The value of the leader in the harmonisation year
# - f_0: The value of the follower at pre-industrial
# - l_0: The value of the leader at pre-industrial
#
# We can then do 'pre-industrial aware scaling' with
#
# f_future =  (l_future - l_0) * (f_harmonisation_year - f_0) / (l_harmonisation_year - l_0) + f_0
#
# so that:
#
# - f_future(l_0) = f_0 i.e. if the lead goes to its pre-industrial value,
#   the result is the follower's pre-industrial value
# - f_future(l_harmonisation_year) = f_harmonisation_year
#   i.e. we preserve harmonisation of the follower
# - there is a linear transition between these two points
#   as the lead variable's emissions change

# %%
scaling_leaders = {
    "Emissions|C3F8": "Emissions|C2F6",
    "Emissions|C4F10": "Emissions|C2F6",
    "Emissions|C5F12": "Emissions|C2F6",
    "Emissions|C7F16": "Emissions|C2F6",
    "Emissions|C8F18": "Emissions|C2F6",
    "Emissions|cC4F8": "Emissions|CF4",
    "Emissions|SO2F2": "Emissions|CF4",
    "Emissions|HFC236fa": "Emissions|HFC245fa",
    "Emissions|HFC152a": "Emissions|HFC4310mee",
    "Emissions|HFC365mfc": "Emissions|HFC134a",
    "Emissions|CH2Cl2": "Emissions|HFC134a",
    "Emissions|CHCl3": "Emissions|C2F6",
    "Emissions|NF3": "Emissions|SF6",
}

# %%
PI_YEAR = 1750

# %%
harmonisation_year = annual_scenario.columns.min()
exp_harmonisation_year = 2023
if harmonisation_year != exp_harmonisation_year:
    raise AssertionError

# %%
infilled_scaling_l = []
for follower, leader in scaling_leaders.items():
    history_leader = gcages_history.loc[pix.isin(variable=leader)]
    history_follower = gcages_history.loc[pix.isin(variable=follower)]

    f_unit_l = history_follower.pix.unique("unit")
    if len(f_unit_l) != 1:
        raise AssertionError
    f_unit = f_unit_l[0].replace("-", "")

    hl_unit_l = history_leader.pix.unique("unit")
    if len(hl_unit_l) != 1:
        raise AssertionError
    hl_unit = hl_unit_l[0].replace("-", "")

    l_harmonisation_year = Q(float(history_leader[harmonisation_year].values.squeeze()), hl_unit)
    f_harmonisation_year = Q(float(history_follower[harmonisation_year].values.squeeze()), f_unit)

    f_0 = Q(float(history_follower[PI_YEAR].values.squeeze()), f_unit)
    l_0 = Q(float(history_leader[PI_YEAR].values.squeeze()), hl_unit)

    scaling_factor = (f_harmonisation_year - f_0) / (l_harmonisation_year - l_0)
    if np.isnan(scaling_factor):
        msg = f"{f_harmonisation_year=} {l_harmonisation_year=} {f_0=} {l_0=}"
        raise AssertionError(msg)

    lead_df = annual_scenario.loc[pix.isin(variable=leader)]
    if lead_df.empty:
        raise AssertionError(f"{leader=}")

    l_unit_l = lead_df.pix.unique("unit")
    if len(l_unit_l) != 1:
        raise AssertionError
    l_unit = l_unit_l[0].replace("-", "")

    lead_arr = Q(lead_df.values, l_unit)

    follow_arr = scaling_factor * (lead_arr - l_0) + f_0

    follow_df = lead_df.copy()
    follow_df.loc[:, :] = follow_arr.to(f_unit).m
    follow_df = follow_df.pix.assign(unit=f_unit)
    follow_df = follow_df.pix.assign(variable=follower)

    infilled_scaling_l.append(follow_df)

infilled_scaling = pix.concat(infilled_scaling_l)
# infilled_scaling

# %%
for follower, leader in scaling_leaders.items():
    pdf = pix.concat(
        [
            annual_scenario.loc[pix.isin(variable=leader)],
            infilled_scaling.loc[pix.isin(variable=follower)],
            gcages_history.loc[pix.isin(variable=[leader, follower])],
        ]
    ).openscm.to_long_data()
    fg = sns.relplot(
        data=pdf,
        x="time",
        y="value",
        hue="scenario",
        col="variable",
        style="unit",
        col_wrap=2,
        col_order=[leader, follower],
        kind="line",
        facet_kws=dict(sharey=False),
        height=2.5,
        aspect=1.25,
    )
    for ax in fg.axes.flatten():
        ax.set_ylim(ymin=0.0)

    plt.show()


# %% [markdown]
# ## Join with history


# %%
def convert_ghg(ghg: str) -> str:
    """Convert GHG to the gcages emissions name"""
    if ghg.startswith("hcfc"):
        return ghg.replace("hcfc", "HCFC")

    if ghg.startswith("halon"):
        return ghg.replace("halon", "Halon")

    res = ghg.upper()
    res = res.replace("CL", "Cl").replace("BR", "Br")

    return res


inverse_emissions_reshaped = inverse_emissions.pix.assign(
    model=model,
    scenario=scenario,
    region="World",
    variable="Emissions|" + inverse_emissions.index.get_level_values("ghg").map(convert_ghg),
).reset_index("ghg", drop=True)
# Make sure naming is correct
_ = inverse_emissions_reshaped.openscm.update_index_levels(
    {
        "variable": partial(
            convert_variable_name,
            from_convention=SupportedNamingConventions.GCAGES,
            to_convention=SupportedNamingConventions.CMIP7_SCENARIOMIP,
        )
    }
)
# inverse_emissions_reshaped

# %%
complete_scenario = pix.concat(
    [
        annual_scenario,
        infilled_scaling,
        inverse_emissions_reshaped.loc[:, annual_scenario.columns],
    ]
).sort_index(axis="columns")
missing = list(gcages_history.pix.unique("variable").difference(complete_scenario.pix.unique("variable")))
if missing:
    raise AssertionError(missing)

# complete_scenario.sort_index()

# %%
history_aligned_to_scenario = gcages_history.reset_index(["model", "scenario"], drop=True).align(
    complete_scenario.reset_index("unit", drop=True)
)[0]

history_aligned_to_scenario_incl_units_l = []
for variable, unit in (
    complete_scenario.index.droplevel(complete_scenario.index.names.difference(["variable", "unit"]))
    .reorder_levels(["variable", "unit"])
    .drop_duplicates()
):
    variable_locator = pix.isin(variable=variable)
    history_aligned_to_scenario_incl_units_l.append(
        history_aligned_to_scenario.loc[variable_locator].pix.convert_unit(unit)
    )

history_aligned_to_scenario_incl_units = pix.concat(history_aligned_to_scenario_incl_units_l)
# history_aligned_to_scenario_incl_units

# %%
complete_emissions = (
    pix.concat(
        [history_aligned_to_scenario_incl_units.loc[:, : complete_scenario.columns.min() - 1], complete_scenario],
        axis="columns",
    )
    .sort_index(axis="columns")
    .dropna(how="any", axis="columns")
)
if complete_emissions.isnull().any().any():
    raise AssertionError

# complete_emissions

# %%
# sns.relplot(
#     data=complete_emissions.openscm.to_long_data(),
#     x="time",
#     y="value",
#     col="variable",
#     col_wrap=3,
#     facet_kws=dict(sharey=False),
#     kind="line",
# )

# %% [markdown]
# ## Save

# %%
out_file_p.parent.mkdir(exist_ok=True, parents=True)
complete_emissions.to_csv(out_file)
