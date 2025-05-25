# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.17.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown] editable=true slideshow={"slide_type": ""}
# # Create WMO-based annual-means
#
# Create annual-means based on WMO.

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
extracted_wmo_data_path: str = "../output-bundles/dev-test/data/interim/wmo-2022/extracted-mixing-ratios.feather"
historical_data_root_dir: str = "../output-bundles/dev-test/data/raw/historical-ghg-concs"
out_file: str = "../output-bundles/dev-test/data/interim/annual-means/wmo-based_ccl4_annual-mean.feather"


# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Parse parameters

# %% editable=true slideshow={"slide_type": ""}
extracted_wmo_data_path_p = Path(extracted_wmo_data_path)
historical_data_root_dir_p = Path(historical_data_root_dir)
out_file_p = Path(out_file)

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Set up

# %% editable=true slideshow={"slide_type": ""}
# Has to be last year of historical data or earlier
harmonisation_year = 2022

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Load data

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ### WMO

# %% editable=true slideshow={"slide_type": ""}
wmo_data_all = pd.read_feather(extracted_wmo_data_path_p)
wmo_data_raw = wmo_data_all.loc[pix.isin(ghg=ghg)]
# wmo_data_raw

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ### CMIP7 historical GHG concentrations

# %% editable=true slideshow={"slide_type": ""}
cmip7_historical_gm_file_l = list(historical_data_root_dir_p.rglob(f"*{ghg}_*gm*.nc"))
if len(cmip7_historical_gm_file_l) != 1:
    raise AssertionError(cmip7_historical_gm_file_l)

cmip7_historical_gm_file = cmip7_historical_gm_file_l[0]
# cmip7_historical_gm_file

# %% editable=true slideshow={"slide_type": ""}
cmip7_historical_gm_ds = xr.load_dataset(cmip7_historical_gm_file)
cmip7_historical_gm = cmip7_historical_gm_ds[ghg]
# cmip7_historical_gm

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Adjust WMO data to middle of year
#
# Raw data is for 1 Jan, we need middle of year.

# %% editable=true slideshow={"slide_type": ""}
wmo_mid_year = (wmo_data_raw + wmo_data_raw.shift(-1, axis="columns")) / 2.0
# Super basic extrapolation, will be fine as it is for 2500
wmo_mid_year[2500] = 2 * wmo_mid_year[2499] - wmo_mid_year[2498]
# ax = wmo_mid_year.loc[:, 2010: 2020].T.plot()
ax = wmo_mid_year.loc[:, :].T.plot()
ax.grid()
plt.show()

wmo_mid_year

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Check harmonisation

# %% editable=true slideshow={"slide_type": ""}
np.testing.assert_allclose(
    cmip7_historical_gm.sel(time=cmip7_historical_gm["time"].dt.year == harmonisation_year),
    wmo_mid_year.loc[:, harmonisation_year],
    rtol=1e-3,
)

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Use WMO data directly for projections
#
# Given we've checked harmonisation,
# no further processing is needed.

# %% editable=true slideshow={"slide_type": ""}
out_file_p.parent.mkdir(parents=True, exist_ok=True)
wmo_mid_year.to_feather(out_file_p)
