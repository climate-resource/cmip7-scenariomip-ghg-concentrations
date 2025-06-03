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
# # Create files for ESGF

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Imports

# %% editable=true slideshow={"slide_type": ""}
from functools import partial
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import openscm_units
import pandas_indexing as pix  # noqa: F401
import pandas_openscm
import pint_xarray
import xarray as xr

from cmip7_scenariomip_ghg_generation.xarray_helpers import (
    calculate_cos_lat_weighted_mean_latitude_only,
    convert_time_to_year_month,
    convert_year_month_to_time,
)

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Parameters

# %% editable=true slideshow={"slide_type": ""} tags=["parameters"]
ghg: str = "ccl4"
cmip_scenario_name: str = "vllo"
model: str = "REMIND-MAGPIE"
scenario: str = "Very Low Overshoot"
global_mean_monthly_file: str = (
    "../output-bundles/dev-test/data/interim/monthly-means/single-concentration-projection_ccl4_monthly-mean.nc"
)
seasonality_file: str = (
    "../output-bundles/dev-test/data/interim/seasonality/single-concentration-projection_ccl4_seasonality-all-years.nc"
)
lat_gradient_file: str = (
    "../output-bundles/dev-test/data/interim/latitudinal-gradient/ccl4_latitudinal-gradient-info.nc"
)
esgf_ready_root_dir: str = "output-bundles/dev-test/data/processed/esgf-ready"


# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Parse parameters

# %% editable=true slideshow={"slide_type": ""}
global_mean_monthly_file_p = Path(global_mean_monthly_file)
seasonality_file_p = Path(seasonality_file)
lat_gradient_file_p = Path(lat_gradient_file)
esgf_ready_root_dir_p = Path(esgf_ready_root_dir)

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Set up

# %% editable=true slideshow={"slide_type": ""}
ur = pint_xarray.setup_registry(openscm_units.unit_registry)
Q = ur.Quantity
pandas_openscm.register_pandas_accessor()

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Load data

# %%
lda = partial(xr.load_dataarray, decode_times=xr.coders.CFDatetimeCoder(use_cftime=True))
lds = partial(xr.load_dataset, decode_times=xr.coders.CFDatetimeCoder(use_cftime=True))

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ### Global-mean monthly

# %%
global_mean_monthly = lda(global_mean_monthly_file_p)
# global_mean_monthly

# %% [markdown]
# ### Seasonality

# %%
seasonality_month_year = lda(seasonality_file_p)
seasonality = convert_year_month_to_time(seasonality_month_year)
# seasonality

# %% [markdown]
# ### Latitudinal gradient info

# %%
lat_grad_info = lds(lat_gradient_file_p)
# lat_grad_info

# %% [markdown]
# ## Create 15-degree grid product

# %% [markdown]
# ### Crunch latitudinal gradient

# %%
lat_grad = (lat_grad_info["eofs"] * lat_grad_info["principal-components-monthly"]).sum("eof")
lat_grad

# %% [markdown]
# ### Combine

# %%
global_mean_monthly_ym = convert_time_to_year_month(global_mean_monthly)
# global_mean_monthly_ym

# %%
seasonality_ym = convert_time_to_year_month(seasonality)
# seasonality_ym

# %%
lat_grad_ym = convert_time_to_year_month(lat_grad)
# lat_grad_ym

# %%
# Quick checks

# %%
np.testing.assert_allclose(seasonality_ym.mean("month"), 0.0, atol=1e-6)

# %%
np.testing.assert_allclose(calculate_cos_lat_weighted_mean_latitude_only(lat_grad_ym), 0.0, atol=1e-8)

# %%
native_grid_ym = global_mean_monthly_ym + seasonality_ym + lat_grad_ym
# native_grid_ym

# %%
native_grid = convert_year_month_to_time(native_grid_ym)
# native_grid

# %% [markdown]
# ### Plot

# %%
print("Colour mesh plot")
native_grid.plot.pcolormesh(x="time", y="lat", cmap="magma_r", levels=100)
plt.show()

# %%
print("Contour plot fewer levels")
native_grid.plot.contour(x="time", y="lat", cmap="magma_r", levels=30)
plt.show()

# %%
print("Concs at different latitudes")
native_grid.sel(lat=[-87.5, 0, 87.5], method="nearest").plot.line(hue="lat", alpha=0.4)
plt.show()

# %%
print("Flying carpet")
fig = plt.figure(figsize=(8, 6))
ax = fig.add_subplot(projection="3d")
tmp = native_grid.copy()
tmp = tmp.assign_coords(time=tmp["time"].dt.year + tmp["time"].dt.month / 12)
(
    tmp.isel(time=range(0, 200)).plot.surface(
        x="time",
        y="lat",
        ax=ax,
        cmap="magma_r",
        levels=30,
        # alpha=0.7,
    )
)
ax.view_init(15, -135, 0)  # type: ignore
plt.tight_layout()
plt.show()

# %% [markdown]
# ## Create derivative products

# %% [markdown]
# ### Global-mean

# %%
global_mean = calculate_cos_lat_weighted_mean_latitude_only(native_grid)
global_mean

# %%
print("Global-mean monthly")
global_mean.plot()  # type: ignore
plt.show()

# %% [markdown]
# ### Hemispheric-means

# %%
hemispheric_means_l = []
for lat_use, lat_sel in (
    (-45.0, native_grid["lat"] < 0),
    (45.0, native_grid["lat"] > 0),
):
    tmp = calculate_cos_lat_weighted_mean_latitude_only(native_grid.sel(lat=lat_sel))
    tmp = tmp.assign_coords(lat=lat_use)
    hemispheric_means_l.append(tmp)

hemispheric_means = xr.concat(hemispheric_means_l, "lat")
# hemispheric_means

# %%
print("Hemsipheric-means monthly")
hemispheric_means.plot(hue="lat")  # type: ignore
plt.show()

# %% [markdown]
# ### Global-, hemispheric-means, annual-means

# %%
global_mean_annual_mean = global_mean.groupby("time.year").mean()
hemispheric_means_annual_mean = hemispheric_means.groupby("time.year").mean()
# hemispheric_means_annual_mean

# %%
print("Annual-means")
global_mean_annual_mean.plot()
plt.show()

hemispheric_means_annual_mean.plot(hue="lat")
plt.show()

# %% [markdown]
# ## Write to ESGF-ready

# %%
assert False, "To implement"
