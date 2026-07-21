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

import openscm_units
import pandas_indexing as pix  # noqa: F401
import pandas_openscm
import pint_xarray
import xarray as xr

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Parameters

# %% editable=true slideshow={"slide_type": ""} tags=["parameters"]
ghg: str = "so2f2"
internal_processing_scenario_name: str = "vl-cf"
esgf_files_start_year: int = 2015
monthly_mean_dir: str = "../output-bundles/1.1.0/data/interim/monthly-means"
seasonality_dir: str = "../output-bundles/1.1.0/data/interim/seasonality"
lat_gradient_dir: str = "../output-bundles/1.1.0/data/interim/latitudinal-gradient"
historical_data_seasonality_lat_gradient_info_root: str = (
    "../output-bundles/dev-test/data/raw/historical-ghg-data-interim"
)
wmo_2022_clean_file: str = "not_used"
out_file_global_mean_monthly: str = (
    "../output-bundles/dev-test/data/interim/monthly-means/modelling-based-projection_so2f2_monthly-mean_vl-cf-hack.nc"
)
out_file_seasonality: str = "../output-bundles/dev-test/data/interim/seasonality/modelling-based-projection_so2f2_seasonality-all-time_vl-cf-hack.nc"
out_file_lat_gradient: str = (
    "../output-bundles/dev-test/data/interim/latitudinal-gradient/so2f2_latitudinal-gradient-info_vl-cf-hack.nc"
)


# %%
ghg: str = "halon1202"
# wmo_2022_clean_file: str = "not_used"
out_file_global_mean_monthly: str = "../output-bundles/dev-test/data/interim/monthly-means/modelling-based-projection_halon1202_monthly-mean_vl-cf-hack.nc"
out_file_seasonality: str = "../output-bundles/dev-test/data/interim/seasonality/modelling-based-projection_halon1202_seasonality-all-time_vl-cf-hack.nc"
out_file_lat_gradient: str = (
    "../output-bundles/dev-test/data/interim/latitudinal-gradient/halon1202_latitudinal-gradient-info_vl-cf-hack.nc"
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

# %%
global_mean_monthly_no_seasonality_base_scenario = get_base(lda(get_file_from_glob(f"*{ghg}*.nc", monthly_mean_dir_p)))
global_mean_monthly_no_seasonality_base_scenario

# %% [markdown]
# ### Seasonality

# %%
# cut historical pre scenario first year
# join together (historical seasonality is already projected out to years)
assert False

# %%
seasonality_base_scenario = get_base(lda(get_file_from_glob(f"*{ghg}*.nc", seasonality_dir_p)))
seasonality_base_scenario

# %%
lda(
    get_file_from_glob(
        f"{ghg}_seasonality_fifteen-degree_allyears-monthly.nc", historical_data_seasonality_lat_gradient_info_root_p
    )
)

# %% [markdown]
# ### Latitudinal gradient info

# %%
lat_grad_info = ssoi(lds(lat_gradient_file_p).pint.quantify(unit_registry=ur))
# lat_grad_info

# %% [markdown]
# ## Process

# %% [markdown]
# ### Global-mean monthly

# %%
# We thought about matching history carefully in 1000.
# No need to repeat that logic here.
# Just cut the data.
global_mean_monthly_no_seasonality_scenario_keep = (
    global_mean_monthly_no_seasonality_scenario.sel(
        time=global_mean_monthly_no_seasonality_scenario["time"].dt.year >= esgf_files_start_year
    )
    .assign_coords({"scenario": internal_processing_scenario_name})
    .expand_dims("scenario")
)
global_mean_monthly_no_seasonality_scenario_keep

# %% [markdown]
# ## Write output

# %%
global_mean_monthly_no_seasonality_scenario_keep.to_netcdf(out_file_global_mean_monthly_p)
print(f"Wrote: {out_file_global_mean_monthly_p}")
