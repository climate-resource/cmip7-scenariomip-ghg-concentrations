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
import pint
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

# %%
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
if ghg != "halon1202":
    # No idea why Malte didn't include halon1202 in historical, but there it is
    cmip7_historical_gm_annual_ds = load_file_from_glob(f"*{ghg}_*gm_1750-*.nc", historical_data_root_dir_p)
    cmip7_historical_gm_annual = cmip7_historical_gm_annual_ds[ghg]
# cmip7_historical_gm_annual

# %% editable=true slideshow={"slide_type": ""}
if ghg != "halon1202":
    cmip7_historical_gm_monthly_ds = load_file_from_glob(f"*{ghg}_*gm_175001-*.nc", historical_data_root_dir_p)
    cmip7_historical_gm_monthly = cmip7_historical_gm_monthly_ds[ghg]
    # cmip7_historical_gm_monthly

# %% editable=true slideshow={"slide_type": ""}
if ghg != "halon1202":
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
if ghg != "halon1202":
    np.testing.assert_allclose(
        cmip7_historical_gm_annual.sel(
            time=cmip7_historical_gm_annual["time"].dt.year == overlap_year
        ).values.squeeze(),
        annual_mean.loc[:, overlap_year],
        rtol=2e-3,
    )

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ### Stitch

# %% editable=true slideshow={"slide_type": ""}
if ghg != "halon1202":
    cmip7_historical_gm_annual_df = pd.DataFrame(
        cmip7_historical_gm_annual.values[np.newaxis, :],
        columns=cmip7_historical_gm_annual["time"].dt.year.values,
        index=pd.MultiIndex.from_tuples([(ghg, cmip7_historical_gm_annual.attrs["units"])], names=["ghg", "unit"]),
    ).rename_axis(columns=annual_mean.columns.name)
    cmip7_historical_gm_annual_df

# %% editable=true slideshow={"slide_type": ""}
if ghg != "halon1202":
    annual_mean_tmp = annual_mean.loc[:, overlap_year + 1 :]
    stitched = pix.concat(
        [cmip7_historical_gm_annual_df.align(annual_mean_tmp)[0].dropna(axis="columns"), annual_mean_tmp],
        axis="columns",
    )

else:
    stitched = annual_mean

stitched.T.plot()
# stitched

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Interpolate

# %% [markdown]
# We have to ensure that the scenarios stay harmonised until the last month of the overlap year.
# Therefore, we use the same wall control point value for all scenarios.
# A a simple choice, we use the average wall control point across all scenarios.

# %%
stitched_units_l = stitched.pix.unique("unit")
if len(stitched_units_l) != 1:
    raise AssertionError(stitched_units_l)

stitched_units = stitched_units_l[0]

fixed_control_point = openscm_units.unit_registry.Quantity(
    stitched.loc[:, overlap_year : overlap_year + 1].mean(axis="columns").mean(), stitched_units
)

# %%


def get_wall_control_points(
    intervals_x: pint.UnitRegistry.Quantity,
    intervals_y: pint.UnitRegistry.Quantity,
    control_points_wall_x: pint.UnitRegistry.Quantity,
) -> pint.UnitRegistry.Quantity:
    """
    Get wall control points including setting our fixed control point
    """
    # Start off with standard implementation
    control_points_wall_y = get_wall_control_points_y_linear_with_flat_override_on_left(
        intervals_x=intervals_x,
        intervals_y=intervals_y,
        control_points_wall_x=control_points_wall_x,
    )

    # Also fix the control point between overlap year month 12 and overlap year + 1 month 1
    fixed_control_point_idxr = np.where(
        control_points_wall_x == openscm_units.unit_registry.Quantity(overlap_year + 1, "yr")
    )
    control_points_wall_y[fixed_control_point_idxr] = fixed_control_point

    return control_points_wall_y


# %%
stitched_monthly_l = []
for scenario, sdf in stitched.groupby("scenario"):
    stitched_monthly_scenario = (
        interpolate_annual_mean_to_monthly(
            values=sdf.values.squeeze(),
            values_units=stitched_units,
            years=sdf.columns.values,
            algorithm=LaiKaplanInterpolator(
                get_wall_control_points_y_from_interval_ys=get_wall_control_points,
                progress_bar=True,
                min_val=openscm_units.unit_registry.Quantity(0, stitched_units),
            ),
            unit_registry=openscm_units.unit_registry,
        )
        .pint.dequantify()
        .assign_coords(scenario=scenario)
    )

    stitched_monthly_l.append(stitched_monthly_scenario)
    # break

stitched_monthly = xr.concat(stitched_monthly_l, dim="scenario")
stitched_monthly

# %%
months_per_year = 12
last_overlap_month = stitched_monthly.sel(
    time=(stitched_monthly["time"].dt.year == overlap_year) & (stitched_monthly["time"].dt.month == months_per_year)
)

np.testing.assert_allclose(
    last_overlap_month.values.squeeze(),
    last_overlap_month.values[0].squeeze(),
    rtol=1e-4,
)

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Check interpolation

# %% editable=true slideshow={"slide_type": ""}
if ghg != "halon1202":
    cmip7_historical_monthly_no_seasonality_time_axis = convert_year_month_to_time(
        cmip7_historical_monthly_no_seasonality,
        day=15,
    )
    # cmip7_historical_monthly_no_seasonality_time_axis

# %% editable=true slideshow={"slide_type": ""}
if ghg != "halon1202":
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
        ax=ax, label="Stitched monthly", alpha=0.3, hue="scenario"
    )

    ax.grid()
    ax.legend()

# %% editable=true slideshow={"slide_type": ""}
if ghg != "halon1202":
    overlap_times = np.intersect1d(cmip7_historical_monthly_no_seasonality_time_axis["time"], stitched_monthly["time"])

    for scenario, sda in stitched_monthly.groupby("scenario"):
        np.testing.assert_allclose(
            # Check overlap except for the last 6 months,
            # where there can be differences because we now have data for 2023
            # rather than using an extrapolation.
            cmip7_historical_monthly_no_seasonality_time_axis.sel(time=overlap_times).isel(time=slice(-6, 0, None)),
            sda.sel(time=overlap_times, scenario=scenario).isel(time=slice(-6, 0, None)),
            atol=1e-8,
            rtol=1e-2,
        )

# %% editable=true slideshow={"slide_type": ""}
fig, ax = plt.subplots()
stitched_monthly.groupby("time.year").mean().plot(ax=ax, hue="scenario")
ax.set_xlim([2000, 2500])
ax.set_xticks(np.arange(2000, 2500 + 1, 50))
ax.grid()

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Prepare output

# %% editable=true slideshow={"slide_type": ""}
out = stitched_monthly
out.name = ghg
out

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Save

# %% editable=true slideshow={"slide_type": ""}
out_file_p.parent.mkdir(exist_ok=True, parents=True)
out.to_netcdf(out_file_p)
out_file_p
