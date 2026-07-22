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
# # Hack together vl-cf inputs
#
# This allows us to make vl-cf match history plus vl from 2015-2500 where we want it to.

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Imports

# %% editable=true slideshow={"slide_type": ""}
from functools import partial
from pathlib import Path

import numpy as np
import openscm_units
import pandas_indexing as pix  # noqa: F401
import pandas_openscm
import pint_xarray
import xarray as xr

from cmip7_scenariomip_ghg_generation.xarray_helpers import (
    convert_year_month_to_time,
)

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Parameters

# %% editable=true slideshow={"slide_type": ""} tags=["parameters"]
ghg: str = "ccl4"
internal_processing_scenario_name: str = "all"
esgf_files_start_year: int = 2015
monthly_mean_dir: str = "../output-bundles/dev-test/data/interim/monthly-means"
seasonality_dir: str = "../output-bundles/dev-test/data/interim/seasonality"
lat_gradient_dir: str = "../output-bundles/dev-test/data/interim/latitudinal-gradient"
historical_data_seasonality_lat_gradient_info_root: str = (
    "../output-bundles/dev-test/data/raw/historical-ghg-data-interim"
)
wmo_2022_clean_file: str = "../output-bundles/dev-test/data/interim/wmo-2022/cleaned-mixing-ratios.feather"
out_file_global_mean_monthly: str = (
    "../output-bundles/dev-test/data/interim/monthly-means/modelling-based-projection_ccl4_monthly-mean_vl-cf-hack.nc"
)
out_file_seasonality: str = "../output-bundles/dev-test/data/interim/seasonality/modelling-based-projection_ccl4_seasonality-all-time_vl-cf-hack.nc"
out_file_lat_gradient: str = (
    "../output-bundles/dev-test/data/interim/latitudinal-gradient/ccl4_latitudinal-gradient-info_vl-cf-hack.nc"
)


# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Parse parameters

# %% editable=true slideshow={"slide_type": ""}
monthly_mean_dir_p = Path(monthly_mean_dir)
seasonality_dir_p = Path(seasonality_dir)
lat_gradient_dir_p = Path(lat_gradient_dir)
historical_data_seasonality_lat_gradient_info_root_p = Path(historical_data_seasonality_lat_gradient_info_root)
if wmo_2022_clean_file != "not_used":
    wmo_2022_clean_file_p = Path(wmo_2022_clean_file)
else:
    wmo_2022_clean_file_p = wmo_2022_clean_file

out_file_global_mean_monthly_p = Path(out_file_global_mean_monthly)
out_file_seasonality_p = Path(out_file_seasonality)
out_file_lat_gradient_p = Path(out_file_lat_gradient)

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Set up

# %% editable=true slideshow={"slide_type": ""}
ur = pint_xarray.setup_registry(openscm_units.unit_registry)
Q = ur.Quantity
pandas_openscm.register_pandas_accessor()

# %%
# Hard-coded
base_scenario = "vl"
base_scenario_start_year_esgf = 2022

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Load data

# %%
lda = partial(xr.load_dataarray, use_cftime=True)
lds = partial(xr.load_dataset, use_cftime=True)


def ssoi(inv, scenario):  # noqa: D103
    return inv.sel(scenario=scenario).drop_vars("scenario")


def get_base(inv):  # noqa: D103
    available_scenarios = list(inv["scenario"].values)
    all_present = "all" in available_scenarios
    base_present = base_scenario in available_scenarios
    if all_present and base_present:
        raise AssertionError
    if not (all_present or base_present):
        raise AssertionError

    if all_present:
        selected = "all"
    else:
        selected = base_scenario

    return inv.sel(scenario=selected).drop_vars("scenario")


def get_file_from_glob(glob: str, base_dir: Path) -> xr.Dataset:
    """
    Get a single file based on a glob pattern
    """
    file_l = list(base_dir.rglob(glob))
    if len(file_l) != 1:
        raise AssertionError(file_l)

    return file_l[0]


# %% [markdown] editable=true slideshow={"slide_type": ""}
# ### Global-mean monthly

# %% editable=true slideshow={"slide_type": ""}
global_mean_monthly_no_seasonality_base_scenario = get_base(
    lda(get_file_from_glob(f"*_{ghg}_*mean.nc", monthly_mean_dir_p))
)
# global_mean_monthly_no_seasonality_base_scenario

# %% [markdown]
# ### Seasonality

# %% editable=true slideshow={"slide_type": ""}
seasonality_base_scenario = get_base(lda(get_file_from_glob(f"*_{ghg}_*seasonality-all-time.nc", seasonality_dir_p)))
# seasonality_base_scenario

# %% editable=true slideshow={"slide_type": ""}
seasonality_history = lda(
    get_file_from_glob(
        f"{ghg}_seasonality_fifteen-degree_allyears-monthly.nc", historical_data_seasonality_lat_gradient_info_root_p
    )
)
# seasonality_history

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ### Latitudinal gradient info

# %% editable=true slideshow={"slide_type": ""}
latitudinal_gradient_base_scenario = get_base(
    lds(get_file_from_glob(f"{ghg}_*latitudinal-gradient-info.nc", lat_gradient_dir_p))
)
# latitudinal_gradient_base_scenario

# %% editable=true slideshow={"slide_type": ""}
latitudinal_gradient_history = lds(
    get_file_from_glob(
        f"{ghg}_latitudinal-gradient_fifteen-degree_allyears-monthly.nc",
        historical_data_seasonality_lat_gradient_info_root_p,
    )
)
# latitudinal_gradient_history

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Process

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ### Global-mean monthly

# %%
# We thought about matching history carefully in 1000.
# No need to repeat that logic here.
# Just cut the data.
global_mean_monthly_no_seasonality_base_scenario_keep = (
    global_mean_monthly_no_seasonality_base_scenario.sel(
        time=global_mean_monthly_no_seasonality_base_scenario["time"].dt.year >= esgf_files_start_year
    )
    .assign_coords({"scenario": internal_processing_scenario_name})
    .expand_dims("scenario")
)
global_mean_monthly_no_seasonality_base_scenario_keep

# %% [markdown]
# ### Seasonality

# %%
seasonality_base_scenario_join = seasonality_base_scenario.sel(
    year=(seasonality_base_scenario["year"] >= base_scenario_start_year_esgf)
)
# seasonality_base_scenario_join

# %%
seasonality_history_join = seasonality_history.sel(
    year=(seasonality_history["year"] < base_scenario_start_year_esgf)
    & (seasonality_history["year"] >= esgf_files_start_year)
)
# seasonality_history_join

# %%
seasonality_out = (
    xr.concat([seasonality_history_join, seasonality_base_scenario_join], dim="year")
    .assign_coords({"scenario": internal_processing_scenario_name})
    .expand_dims("scenario")
)
np.testing.assert_array_equal(
    seasonality_out["year"].values, np.arange(esgf_files_start_year, seasonality_base_scenario["year"].max() + 1)
)

# %% [markdown]
# ### Latitudinal gradient

# %%
latitudinal_gradient_base_scenario_join = latitudinal_gradient_base_scenario.sel(
    time=(latitudinal_gradient_base_scenario["time"].dt.year >= base_scenario_start_year_esgf)
)
# latitudinal_gradient_base_scenario_join

# %% editable=true slideshow={"slide_type": ""}
latitudinal_gradient_history_join = convert_year_month_to_time(
    latitudinal_gradient_history.sel(
        year=(latitudinal_gradient_history["year"] < base_scenario_start_year_esgf)
        & (latitudinal_gradient_history["year"] >= esgf_files_start_year)
    )
)
# latitudinal_gradient_history_join

# %% editable=true slideshow={"slide_type": ""}
# Calculate latitudinal gradient.
latitudinal_gradient_base_scenario_join_no_eof = (
    latitudinal_gradient_base_scenario_join["eofs"]
    * latitudinal_gradient_base_scenario_join["principal-components-monthly"]
).sum("eof")
# Then join.
latitudinal_gradient_out_no_eof = xr.concat(
    [latitudinal_gradient_history_join["latitudinal-gradient"], latitudinal_gradient_base_scenario_join_no_eof],
    dim="time",
)
# Then fake an EOF back in so notebook 1100 doesn't explode
fake_eof = xr.DataArray(
    np.ones_like(latitudinal_gradient_out_no_eof["lat"])[:, np.newaxis],
    coords={"eof": [0], "lat": latitudinal_gradient_out_no_eof["lat"]},
    dims=("lat", "eof"),
    name="eofs",
)

latitudinal_gradient_out = xr.merge(
    [latitudinal_gradient_out_no_eof.to_dataset(name="principal-components-monthly"), fake_eof]
)

# Check everything
xr.testing.assert_allclose(
    (latitudinal_gradient_out["eofs"] * latitudinal_gradient_out["principal-components-monthly"]).sum("eof"),
    latitudinal_gradient_out_no_eof,
)

latitudinal_gradient_out = latitudinal_gradient_out.assign_coords(
    {"scenario": internal_processing_scenario_name}
).expand_dims("scenario")
# latitudinal_gradient_out

# %% [markdown]
# ## Write output

# %%
global_mean_monthly_no_seasonality_base_scenario_keep.to_netcdf(out_file_global_mean_monthly_p)
print(f"Wrote: {out_file_global_mean_monthly_p}")

# %%
seasonality_out.to_netcdf(out_file_seasonality_p)
print(f"Wrote: {out_file_seasonality_p}")

# %%
latitudinal_gradient_out.to_netcdf(out_file_lat_gradient_p)
print(f"Wrote: {out_file_lat_gradient_p}")
