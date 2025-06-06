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
# Interpolate to annual,
# do scaling-based infilling
# and join with history.

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Imports

# %% editable=true slideshow={"slide_type": ""}
from functools import partial
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr
from pandas_openscm.io import load_timeseries_csv

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Parameters

# %% editable=true slideshow={"slide_type": ""} tags=["parameters"]
model: str = "MESSAGEix-GLOBIOM-GAINS 2.1-M-R12"
scenario: str = "SSP2 - Low Emissions"
scenario_file: str = "../output-bundles/dev-test/data/interim/input-emissions/0009-zn_0003_0003_0002/SSP2_-_Low_Emissions_MESSAGEix-GLOBIOM-GAINS_2-1-M-R12.feather"
history_file: str = "../output-bundles/dev-test/data/interim/input-emissions/0009-zn_0003_0003_0002/historical.feather"
out_file: str = "../output-bundles/dev-test/data/processed/complete-emissions/SSP2_-_Low_Emissions_MESSAGEix-GLOBIOM-GAINS_2-1-M-R12.csv"


# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Parse parameters

# %% editable=true slideshow={"slide_type": ""}
scenario_file_p = Path(scenario_file)
history_file_p = Path(history_file)
out_file_p = Path(out_file)

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Set up

# %% editable=true slideshow={"slide_type": ""}

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
# ### Scenario without infilling

# %% editable=true slideshow={"slide_type": ""}
raw_scenario = pd.read_feather(scenario_file_p)
# raw_scenario

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ### CMIP7 historical GHG concentrations

# %% editable=true slideshow={"slide_type": ""}
cmip7_historical_gm_file_l = list(historical_data_root_dir_p.rglob(f"*{ghg}_*gm_1750-*.nc"))
if len(cmip7_historical_gm_file_l) != 1:
    raise AssertionError(cmip7_historical_gm_file_l)

cmip7_historical_gm_file = cmip7_historical_gm_file_l[0]
# cmip7_historical_gm_file

# %% editable=true slideshow={"slide_type": ""}
cmip7_historical_gm_ds = xr.load_dataset(cmip7_historical_gm_file)
cmip7_historical_gm = cmip7_historical_gm_ds[ghg]
# cmip7_historical_gm

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Adjust raw data to middle of year
#
# Assumption: raw data is for 1 Jan, we need middle of year.
# Fine as we check alignment of historical and source later.

# %% editable=true slideshow={"slide_type": ""}
source_mid_year = (raw_data_ghg + raw_data_ghg.shift(-1, axis="columns")) / 2.0
# Super basic extrapolation, will be fine as it is for 2500
source_mid_year[2500] = 2 * source_mid_year[2499] - source_mid_year[2498]
# ax = wmo_mid_year.loc[:, 2010: 2020].T.plot()
ax = source_mid_year.loc[:, :].T.plot()
ax.grid()
plt.show()

source_mid_year

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Harmonisation

# %% editable=true slideshow={"slide_type": ""}
last_history_year = int(cmip7_historical_gm["time"].dt.year.max())
last_history_year

# %%
fig, ax = plt.subplots()

cmm = cmip7_historical_gm.groupby("time.year").mean()
ax.plot(
    cmm["year"],
    cmm.values,
    label="CMIP7 historical",
)
ax.plot(source_mid_year.columns, source_mid_year.values.squeeze(), label="WMO 2022", linewidth=2, alpha=0.5)

ax.set_xlim([2000, 2050])
ax.grid()
ax.legend()
ax.set_title(ghg)

# %%
np.testing.assert_allclose(
    cmip7_historical_gm.sel(time=cmip7_historical_gm["time"].dt.year == last_history_year),
    source_mid_year.loc[:, last_history_year],
    rtol=2e-3,
)

harmonised = source_mid_year

# %% editable=true slideshow={"slide_type": ""}
fig, ax = plt.subplots()


source_mid_year.pix.assign(label="WMO 2022").pix.project(["label", "ghg", "unit"]).T.plot(ax=ax, linewidth=2, alpha=0.5)
harmonised.pix.assign(label="harmonised").pix.project(["label", "ghg", "unit"]).T.plot(ax=ax, linewidth=2, alpha=0.5)
ax.plot(
    cmm["year"],
    cmm.values,
    label="CMIP7 historical",
)

ax.set_xlim([2000, 2100])
ax.grid()
ax.legend()

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Save

# %% editable=true slideshow={"slide_type": ""}
out_file_p.parent.mkdir(parents=True, exist_ok=True)
harmonised.loc[:, last_history_year:].to_feather(out_file_p)
