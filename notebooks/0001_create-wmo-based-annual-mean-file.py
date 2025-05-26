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
from gradient_aware_harmonisation import Timeseries
from gradient_aware_harmonisation.add_cubic import harmonise_splines_add_cubic

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Parameters

# %% editable=true slideshow={"slide_type": ""} tags=["parameters"]
ghg: str = "hcfc22"
harmonise: bool = True
extracted_wmo_data_path: str = "../output-bundles/dev-test/data/interim/wmo-2022/extracted-mixing-ratios.feather"
historical_data_root_dir: str = "../output-bundles/dev-test/data/raw/historical-ghg-concs"
out_file: str = "../output-bundles/dev-test/data/interim/annual-means/wmo-based_hcfc142b_annual-mean.feather"


# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Parse parameters

# %% editable=true slideshow={"slide_type": ""}
extracted_wmo_data_path_p = Path(extracted_wmo_data_path)
historical_data_root_dir_p = Path(historical_data_root_dir)
out_file_p = Path(out_file)

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Set up

# %% editable=true slideshow={"slide_type": ""}

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Load data

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ### WMO

# %% editable=true slideshow={"slide_type": ""}
wmo_data_all = pd.read_feather(extracted_wmo_data_path_p)
wmo_data_all.columns = wmo_data_all.columns.astype(int)
wmo_data_raw = wmo_data_all.loc[pix.isin(ghg=ghg)]
# wmo_data_raw.columns

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
ax.plot(wmo_mid_year.columns, wmo_mid_year.values.squeeze(), label="WMO 2022", linewidth=2, alpha=0.5)

ax.set_xlim([2000, 2050])
ax.grid()
ax.legend()
ax.set_title(ghg)

# %%
if not harmonise:
    np.testing.assert_allclose(
        cmip7_historical_gm.sel(time=cmip7_historical_gm["time"].dt.year == last_history_year),
        wmo_mid_year.loc[:, last_history_year],
        rtol=2e-3,
    )

    harmonised = wmo_mid_year

# %%
if harmonise:
    wmo_spline = Timeseries(
        time_axis=wmo_mid_year.columns,
        values=wmo_mid_year.values.squeeze(),
    ).to_spline(bc_type=None)

    cmip7_historical_spline = Timeseries(
        time_axis=cmm["year"],
        values=cmm.values.squeeze(),
    ).to_spline(bc_type=None)

    harmonised_spline = harmonise_splines_add_cubic(
        diverge_from=cmip7_historical_spline,
        harmonisee=wmo_spline,
        harmonisation_time=last_history_year,
        convergence_time=2030,
    )

    # from gradient_aware_harmonisation import harmonise_splines, get_cosine_decay_harmonised_spline

    # harmonised_spline = harmonise_splines(
    #     target=cmip7_historical_spline,
    #     harmonisee=wmo_spline,
    #     converge_to=wmo_spline,
    #     harmonisation_time=last_history_year,
    #     convergence_time=2100,
    #     get_harmonised_spline=get_cosine_decay_harmonised_spline
    # )

    fig, ax = plt.subplots()

    ax.plot(
        cmm["year"],
        cmm.values,
        label="CMIP7 historical",
        linewidth=2,
        alpha=0.5,
        zorder=3,
    )
    cmm_fine = np.linspace(cmm["year"].min().values, cmm["year"].max().values, 5000)
    ax.plot(
        cmm_fine,
        cmip7_historical_spline(cmm_fine),
        label="CMIP7 historical spline",
    )

    ax.plot(wmo_mid_year.columns, wmo_mid_year.values.squeeze(), label="WMO 2022", linewidth=2, alpha=0.5)
    wmo_mid_year_fine = np.linspace(wmo_mid_year.columns.min(), wmo_mid_year.columns.max(), 5000)
    ax.plot(
        wmo_mid_year_fine,
        wmo_spline(wmo_mid_year_fine),
        label="WMO 2022 spline",
    )

    harmonised = wmo_mid_year.loc[:, last_history_year:].copy()
    harmonised.loc[:] = harmonised_spline(harmonised.columns)
    ax.plot(harmonised.columns, harmonised.values.squeeze(), label="Harmonised", linewidth=2, alpha=0.5)

    harmonised_year_fine = np.linspace(last_history_year, wmo_mid_year.columns.max(), 5000)
    ax.plot(
        harmonised_year_fine,
        harmonised_spline(harmonised_year_fine),
        label="Harmonised spline",
    )

    ax.set_xlim([2000, 2100])
    ax.grid()
    ax.legend()

    assert False, "Update based on reply from Luke and others"

# %% editable=true slideshow={"slide_type": ""}
fig, ax = plt.subplots()


wmo_mid_year.pix.assign(label="WMO 2022").pix.project(["label", "ghg", "unit"]).T.plot(ax=ax, linewidth=2, alpha=0.5)
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
