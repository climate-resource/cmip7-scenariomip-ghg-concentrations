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
# # Interpolate annual-means to monthly mean
#
# Requires mean-preserving interpolation,
# hence is more fiddly than one would expect.

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Imports

# %% editable=true slideshow={"slide_type": ""}
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import openscm_units
import pandas as pd
import pandas_indexing as pix
import pint_xarray  # noqa: F401
import xarray as xr

from cmip7_scenariomip_ghg_generation.mean_preserving_interpolation import (
    LaiKaplanInterpolator,
    interpolate_annual_mean_to_monthly,
)
from cmip7_scenariomip_ghg_generation.mean_preserving_interpolation.lai_kaplan import (
    get_wall_control_points_y_linear_with_flat_override_on_left,
)
from cmip7_scenariomip_ghg_generation.xarray_helpers import convert_year_month_to_time

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Parameters

# %% editable=true slideshow={"slide_type": ""} tags=["parameters"]
ghg: str = "ccl4"
annual_mean_file: str = (
    "../output-bundles/dev-test/data/interim/annual-means/single-concentration-projection_ccl4_annual-mean.feather"
)
historical_data_root_dir: str = "../output-bundles/dev-test/data/raw/historical-ghg-concs"
historical_data_seasonality_lat_gradient_info_root: str = (
    "../output-bundles/dev-test/data/raw/historical-ghg-data-interim"
)
out_file: str = (
    "../output-bundles/dev-test/data/interim/monthly-means/single-concentration-projection_ccl4_monthly-mean.nc"
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
cmip7_historical_gm_annual_ds = load_file_from_glob(f"*{ghg}_*gm_1750-*.nc", historical_data_root_dir_p)
cmip7_historical_gm_annual = cmip7_historical_gm_annual_ds[ghg]
# cmip7_historical_gm_annual

# %% editable=true slideshow={"slide_type": ""}
cmip7_historical_gm_monthly_ds = load_file_from_glob(f"*{ghg}_*gm_175001-*.nc", historical_data_root_dir_p)
cmip7_historical_gm_monthly = cmip7_historical_gm_monthly_ds[ghg]
# cmip7_historical_gm_monthly

# %% editable=true slideshow={"slide_type": ""}
cmip7_historical_monthly_no_seasonality_ds = load_file_from_glob(
    f"{ghg}_global-annual-mean_allyears-monthly.nc", historical_data_seasonality_lat_gradient_info_root_p
)
cmip7_historical_monthly_no_seasonality = (
    cmip7_historical_monthly_no_seasonality_ds.to_dataarray().isel(variable=0).drop_vars("variable")
)
# cmip7_historical_monthly_no_seasonality

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Stitch history and projections

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ### Check harmonised

# %% editable=true slideshow={"slide_type": ""}
overlap_year = annual_mean.columns.min()
# overlap_year

# %% editable=true slideshow={"slide_type": ""}
np.testing.assert_allclose(
    cmip7_historical_gm_annual.sel(time=cmip7_historical_gm_annual["time"].dt.year == overlap_year),
    annual_mean.loc[:, overlap_year],
    rtol=2e-3,
)

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ### Stitch

# %% editable=true slideshow={"slide_type": ""}
cmip7_historical_gm_annual_df = pd.DataFrame(
    cmip7_historical_gm_annual.values[np.newaxis, :],
    columns=cmip7_historical_gm_annual["time"].dt.year.values,
    index=pd.MultiIndex.from_tuples([(ghg, cmip7_historical_gm_annual.attrs["units"])], names=["ghg", "unit"]),
).rename_axis(columns=annual_mean.columns.name)
# cmip7_historical_gm_annual_df

# %% editable=true slideshow={"slide_type": ""}
stitched = pix.concat([cmip7_historical_gm_annual_df, annual_mean.loc[:, overlap_year + 1 :]], axis="columns")
stitched.T.plot()

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Interpolate

# %% editable=true slideshow={"slide_type": ""}
stitched_units_l = stitched.pix.unique("unit")
if len(stitched_units_l) != 1:
    raise AssertionError(stitched_units_l)

stitched_units = stitched_units_l[0]

stitched_monthly = interpolate_annual_mean_to_monthly(
    values=stitched.values.squeeze(),
    values_units=stitched_units,
    years=stitched.columns.values,
    algorithm=LaiKaplanInterpolator(
        get_wall_control_points_y_from_interval_ys=get_wall_control_points_y_linear_with_flat_override_on_left,
        progress_bar=True,
        min_val=openscm_units.unit_registry.Quantity(0, stitched_units),
    ),
    unit_registry=openscm_units.unit_registry,
).pint.dequantify()
stitched_monthly

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Check interpolation

# %% editable=true slideshow={"slide_type": ""}
cmip7_historical_monthly_no_seasonality_time_axis = convert_year_month_to_time(
    cmip7_historical_monthly_no_seasonality,
    day=15,
)
# cmip7_historical_monthly_no_seasonality_time_axis

# %% editable=true slideshow={"slide_type": ""}
fig, ax = plt.subplots()

years_to_plot = np.arange(overlap_year - 4, overlap_year + 1)
cmip7_historical_gm_monthly.sel(
    time=cmip7_historical_gm_monthly["time"].dt.year.isin(np.arange(overlap_year - 4, overlap_year + 1))
).plot.scatter(ax=ax, label="CMIP7 hist incl. seasonality", alpha=0.6)
cmip7_historical_monthly_no_seasonality_time_axis.sel(
    time=cmip7_historical_monthly_no_seasonality_time_axis["time"].dt.year.isin(
        np.arange(overlap_year - 4, overlap_year + 1)
    )
).plot.scatter(ax=ax, label="CMIP7 hist excl. seasonality", alpha=0.6)

years_to_plot = np.arange(overlap_year - 4, overlap_year + 5)
stitched_monthly.sel(time=stitched_monthly["time"].dt.year.isin(years_to_plot)).plot.scatter(
    ax=ax, label="Stitched monthly", alpha=0.3
)

ax.grid()
ax.legend()

# %% editable=true slideshow={"slide_type": ""}
overlap_times = np.intersect1d(cmip7_historical_monthly_no_seasonality_time_axis["time"], stitched_monthly["time"])

np.testing.assert_allclose(
    cmip7_historical_monthly_no_seasonality_time_axis.sel(time=overlap_times),
    stitched_monthly.sel(time=overlap_times),
    atol=1e-8,
    rtol=1e-2,
)

# %% editable=true slideshow={"slide_type": ""}
fig, ax = plt.subplots()
stitched_monthly.groupby("time.year").mean().plot(ax=ax)
ax.set_xlim([2000, 2500])
ax.set_xticks(np.arange(2000, 2500 + 1, 50))
ax.grid()

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Prepare output

# %% editable=true slideshow={"slide_type": ""}
out = stitched_monthly.sel(time=stitched_monthly["time"].dt.year.isin(annual_mean.loc[:, overlap_year:].columns))
out.name = ghg
out

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Save

# %% editable=true slideshow={"slide_type": ""}
out_file_p.parent.mkdir(exist_ok=True, parents=True)
out.to_netcdf(out_file_p)
out_file_p
