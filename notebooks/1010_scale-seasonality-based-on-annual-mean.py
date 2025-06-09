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
# # Scale seasonality based on annual-mean

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Imports

# %% editable=true slideshow={"slide_type": ""}
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import openscm_units
import pandas as pd
import pandas_indexing as pix  # noqa: F401
import pint_xarray
import xarray as xr

from cmip7_scenariomip_ghg_generation.xarray_helpers import (
    convert_year_month_to_time,
)

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Parameters

# %% editable=true slideshow={"slide_type": ""} tags=["parameters"]
ghg: str = "hfc236fa"
annual_mean_file: str = "../output-bundles/dev-test/data/interim/annual-means/one-box_hfc236fa_annual-mean.feather"
historical_data_root_dir: str = "../output-bundles/dev-test/data/raw/historical-ghg-concs"
historical_data_seasonality_lat_gradient_info_root: str = (
    "../output-bundles/dev-test/data/raw/historical-ghg-data-interim"
)
out_file: str = (
    "../output-bundles/dev-test/data/interim/seasonality/modelling-based-projection_hfc236fa_seasonality-all-time.nc"
)


# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Parse parameters

# %% editable=true slideshow={"slide_type": ""}
annual_mean_file_p = Path(annual_mean_file)
historical_data_root_dir_p = Path(historical_data_root_dir)
historical_data_seasonality_lat_gradient_info_root_p = Path(historical_data_seasonality_lat_gradient_info_root)
out_file_p = Path(out_file)

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Set up

# %% editable=true slideshow={"slide_type": ""}
ur = pint_xarray.setup_registry(openscm_units.unit_registry)
Q = ur.Quantity

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Load data

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ### Annual-mean file

# %% editable=true slideshow={"slide_type": ""}
annual_mean = pd.read_feather(annual_mean_file_p)
annual_mean


# %% [markdown] editable=true slideshow={"slide_type": ""}
# ### CMIP7 historical GHG concentrations


# %% editable=true slideshow={"slide_type": ""}
def load_file_from_glob(glob: str, base_dir: Path) -> xr.Dataset:
    """
    Load a single file based on a glob pattern
    """
    file_l = list(base_dir.rglob(glob))
    if len(file_l) != 1:
        raise AssertionError(file_l)

    ds = xr.load_dataset(file_l[0])

    return ds


# %% editable=true slideshow={"slide_type": ""}
cmip7_historical_gm_monthly_ds = load_file_from_glob(f"*{ghg}_*gm_175001-*.nc", historical_data_root_dir_p)
cmip7_historical_gm_monthly = cmip7_historical_gm_monthly_ds[ghg]
# cmip7_historical_gm_monthly

# %% [markdown]
# ### CMIP7 historical seasonality

# %%
cmip7_seasonality_all_years_ds = load_file_from_glob(
    f"{ghg}_seasonality_fifteen-degree_allyears-monthly.nc", historical_data_seasonality_lat_gradient_info_root_p
).pint.quantify(unit_registry=openscm_units.unit_registry)
cmip7_seasonality_all_years = cmip7_seasonality_all_years_ds.to_dataarray().isel(variable=0).drop_vars("variable")
# cmip7_seasonality_all_years

# %% editable=true slideshow={"slide_type": ""}
try:
    cmip7_seasonality_ds = load_file_from_glob(
        f"{ghg}_observational-network_seasonality.nc", historical_data_seasonality_lat_gradient_info_root_p
    ).pint.quantify(unit_registry=openscm_units.unit_registry)
    cmip7_seasonality = cmip7_seasonality_ds.to_dataarray().isel(variable=0).drop_vars("variable")
    # cmip7_seasonality
except AssertionError:
    if not (cmip7_seasonality_all_years == 0.0).all():
        msg = "Expected to find zero seasonality"
        raise AssertionError(msg)

    # Ensure we get relative units
    cmip7_seasonality = cmip7_seasonality_all_years.isel(year=0) / (1 * cmip7_seasonality_all_years.data[0].u)

# %% [markdown]
# ## Check scaling

# %%
annual_mean_first_year = annual_mean.columns.min()
annual_mean_first_year_value_all_scenarios = annual_mean[annual_mean_first_year]
np.testing.assert_allclose(
    annual_mean_first_year_value_all_scenarios.values,
    annual_mean_first_year_value_all_scenarios.values[0],
)
# Can use any scenario as they're all close
annual_mean_first_year_value = annual_mean_first_year_value_all_scenarios.values[0]
annual_mean_first_year_value

# %%
np.testing.assert_allclose(
    cmip7_seasonality_all_years.sel(year=annual_mean_first_year).data.m,
    # Product of seasonality and annual-mean
    # should give the seasonality that was actually used.
    cmip7_seasonality.data.m * annual_mean_first_year_value,
    # Rounding means these differences can be surprisingly big.
    # This still makes sure we're in the right ballpark.
    atol=1e-3,
    rtol=1e-2,
)

# %%
annual_mean_unit_l = annual_mean.pix.unique("unit")
if len(annual_mean_unit_l) != 1:
    raise AssertionError(annual_mean_unit_l)

annual_mean_unit = annual_mean_unit_l[0]

annual_mean_xr = xr.DataArray(
    annual_mean.values,
    dims=["scenario", "year"],
    coords=dict(scenario=annual_mean.index.get_level_values("scenario"), year=annual_mean.columns),
).pint.quantify(annual_mean_unit, unit_registry=openscm_units.unit_registry)

# annual_mean_xr

# %%
seasonality = cmip7_seasonality * annual_mean_xr
# seasonality

# %%
for years in (
    range(2025, 2030 + 1),
    seasonality["year"][-5:],
):
    fg = convert_year_month_to_time(seasonality.sel(year=years, lat=[-82.5, -22.5, 7.5, 82.5])).plot(
        col="scenario", col_wrap=min(3, seasonality["scenario"].size), hue="lat", alpha=0.5
    )
    for ax in fg.axs.flatten():
        ax.grid()

    plt.show()

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Prepare output

# %%
out_years = np.setdiff1d(annual_mean.columns, cmip7_historical_gm_monthly["time"].dt.year.values)
# out_years

# %% editable=true slideshow={"slide_type": ""}
out = seasonality.sel(year=out_years).pint.dequantify()
out.name = f"{ghg}_seasonality_all_months"
out

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Save

# %% editable=true slideshow={"slide_type": ""}
out_file_p.parent.mkdir(exist_ok=True, parents=True)
out.to_netcdf(out_file_p)
out_file_p
