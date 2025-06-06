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
# # Create annual-means for gases based on a single concentration projection
#
# In other words, create annual-means based on WMO 2022 and Western et al. 2024.

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Imports

# %% editable=true slideshow={"slide_type": ""}
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pandas_indexing as pix
import xarray as xr

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Parameters

# %% editable=true slideshow={"slide_type": ""} tags=["parameters"]
ghg: str = "cfc11"
cleaned_data_path: str = "../output-bundles/dev-test/data/interim/wmo-2022/extracted-mixing-ratios.feather"
historical_data_root_dir: str = "../output-bundles/dev-test/data/raw/historical-ghg-concs"
out_file: str = "../output-bundles/dev-test/data/interim/annual-means/wmo-based_cfc11_annual-mean.feather"


# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Parse parameters

# %% editable=true slideshow={"slide_type": ""}
cleaned_data_path_p = Path(cleaned_data_path)
historical_data_root_dir_p = Path(historical_data_root_dir)
out_file_p = Path(out_file)

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Set up

# %% editable=true slideshow={"slide_type": ""}

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Load data

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ### Cleaned data

# %% editable=true slideshow={"slide_type": ""}
raw_data_all = pd.read_feather(cleaned_data_path_p)
raw_data_all.columns = raw_data_all.columns.astype(int)
raw_data_ghg = raw_data_all.loc[pix.isin(ghg=ghg)]
# raw_data_ghg.columns

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ### CMIP7 historical GHG concentrations

# %% editable=true slideshow={"slide_type": ""}
if ghg != "halon1202":
    # No idea why Malte didn't include halon1202 in historical, but there it is
    cmip7_historical_gm_file_l = list(historical_data_root_dir_p.rglob(f"*{ghg}_*gm_1750-*.nc"))
    if len(cmip7_historical_gm_file_l) != 1:
        raise AssertionError(cmip7_historical_gm_file_l)

    cmip7_historical_gm_file = cmip7_historical_gm_file_l[0]
    # cmip7_historical_gm_file

# %% editable=true slideshow={"slide_type": ""}
if ghg != "halon1202":
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
last_history_year_exp = 2022
if ghg != "halon1202":
    last_history_year = int(cmip7_historical_gm["time"].dt.year.max())
else:
    last_history_year = last_history_year_exp

if last_history_year != last_history_year_exp:
    msg = "Please check last historical GHG conc year"
    raise AssertionError(msg)

last_history_year

# %%
fig, ax = plt.subplots()

if ghg != "halon1202":
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
if ghg != "halon1202":
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
if ghg != "halon1202":
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
