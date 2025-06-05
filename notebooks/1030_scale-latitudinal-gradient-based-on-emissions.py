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
# # Scale latitudinal gradient based on emissions

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Imports

# %% editable=true slideshow={"slide_type": ""}
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import openscm_units
import pandas as pd
import pandas_indexing as pix  # noqa: F401
import pandas_openscm
import pint_xarray
import xarray as xr
import yaml

from cmip7_scenariomip_ghg_generation.mean_preserving_interpolation import (
    LaiKaplanInterpolator,
    interpolate_annual_mean_to_monthly,
)
from cmip7_scenariomip_ghg_generation.xarray_helpers import (
    convert_year_to_time,
)

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Parameters

# %% editable=true slideshow={"slide_type": ""} tags=["parameters"]
ghg: str = "ccl4"
annual_mean_emissions_file: str = "../output-bundles/dev-test/data/processed/inverse-emissions/single-concentration-projection_ccl4_inverse-emissions.feather"  # noqa: E501
historical_data_root_dir: str = "../output-bundles/dev-test/data/raw/historical-ghg-concs"
historical_data_seasonality_lat_gradient_info_root: str = (
    "../output-bundles/dev-test/data/raw/historical-ghg-data-interim"
)
out_file: str = "../output-bundles/dev-test/data/interim/latitudinal-gradient/ccl4_latitudinal-gradient-info.nc"


# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Parse parameters

# %% editable=true slideshow={"slide_type": ""}
annual_mean_emissions_file_p = Path(annual_mean_emissions_file)
historical_data_root_dir_p = Path(historical_data_root_dir)
historical_data_seasonality_lat_gradient_info_root_p = Path(historical_data_seasonality_lat_gradient_info_root)
out_file_p = Path(out_file)

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Set up

# %% editable=true slideshow={"slide_type": ""}
ur = pint_xarray.setup_registry(openscm_units.unit_registry)
Q = ur.Quantity
pandas_openscm.register_pandas_accessor()

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Load data

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ### Annual-mean emissions

# %% editable=true slideshow={"slide_type": ""}
annual_mean_emissions = pd.read_feather(annual_mean_emissions_file_p)
annual_mean_emissions_emms_units = annual_mean_emissions.loc[
    annual_mean_emissions.index.get_level_values("unit").str.lower().str.contains(ghg)
]
if annual_mean_emissions_emms_units.shape[0] != 1:
    raise AssertionError

annual_mean_emissions_emms_units


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
# ### CMIP7 historical latitudinal gradient

# %%
cmip7_lat_gradient_pieces_ds = load_file_from_glob(
    f"{ghg}_allyears-lat-gradient-eofs-pcs.nc", historical_data_seasonality_lat_gradient_info_root_p
).pint.quantify(unit_registry=ur)
# cmip7_seasonality_all_years = cmip7_seasonality_all_years_ds.to_dataarray().isel(variable=0).drop_vars("variable")
# # cmip7_seasonality_all_years
cmip7_lat_gradient_pieces_ds

# %% [markdown]
# ### CMIP7 historical latitudinal gradient regression info

# %%
file_l = list(historical_data_seasonality_lat_gradient_info_root_p.rglob(f"{ghg}_pc0-total-emissions-regression.yaml"))
if len(file_l) != 1:
    raise AssertionError(file_l)

# %%
with open(file_l[0]) as fh:
    reg_info = yaml.safe_load(fh)

reg_info

# %% [markdown]
# ## Scale latitudinal gradient pc

# %%
last_hist_year = cmip7_historical_gm_monthly["time"].dt.year.values[-1]
# last_hist_year

# %%
pc = cmip7_lat_gradient_pieces_ds["principal-components"]
if pc["eof"].shape != (1,):
    msg = "Expect only one EOF"
    raise AssertionError(msg)

# pc

# %%
delta_E = (
    annual_mean_emissions_emms_units.loc[:, last_hist_year:].subtract(
        annual_mean_emissions_emms_units[last_hist_year], axis="rows"
    )
).pix.assign(variable="change_in_emissions")
delta_E

# %%
delta_E_unit_l = delta_E.pix.unique("unit")
if len(delta_E_unit_l) != 1:
    raise AssertionError(delta_E_unit_l)

delta_E_unit = delta_E_unit_l[0]

delta_E_xr = xr.DataArray(
    delta_E.values.squeeze(),
    dims=["year"],
    coords=dict(year=delta_E.columns),
).pint.quantify(delta_E_unit, unit_registry=ur)
# dE_dt_xr

# %%
delta_pc = delta_E_xr * Q(reg_info["m"][0], reg_info["m"][1])
# delta_pc

# %%
pc_extended = delta_pc + pc.sel(year=last_hist_year).data

fig, axes = plt.subplots(nrows=2)
annual_mean_emissions_emms_units.T.plot(ax=axes[0])
pc_extended.pint.to("dimensionless").plot(ax=axes[1])
pc.pint.to("dimensionless").plot(ax=axes[1])

# %% [markdown]
# ## Interpolate to monthly

# %%
pc_extended_monthly = interpolate_annual_mean_to_monthly(
    values=pc_extended.data.m.squeeze(),
    values_units=pc_extended.data.u,
    years=pc_extended["year"].values,
    algorithm=LaiKaplanInterpolator(
        progress_bar=True,
    ),
    unit_registry=openscm_units.unit_registry,
)
pc_extended_monthly.name = "principal-components-monthly"
pc_extended_monthly.attrs["description"] = "principal component values on a monthly timestep"
pc_extended_monthly

# %%
fig, axes = plt.subplots(ncols=2)

for years, ax in (
    (np.arange(last_hist_year + 1, 2500 + 1), axes[0]),
    (np.arange(last_hist_year + 1, last_hist_year + 15), axes[1]),
):
    pc_extended_monthly.sel(time=pc_extended_monthly["time"].dt.year.isin(years)).pint.to("dimensionless").plot(
        ax=ax, alpha=0.6
    )
    convert_year_to_time(pc_extended.sel(year=years)).pint.to("dimensionless").plot.scatter(ax=ax)

plt.tight_layout()

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Prepare output

# %%
out = xr.merge(
    [
        cmip7_lat_gradient_pieces_ds["eofs"],
        pc_extended_monthly.pint.to("dimensionless"),
    ],
    combine_attrs="drop_conflicts",
).pint.dequantify()

out

# %%
# Double check this can work
tmp = out["eofs"] * out["principal-components-monthly"]
# tmp

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Save

# %% editable=true slideshow={"slide_type": ""}
out_file_p.parent.mkdir(exist_ok=True, parents=True)
out.to_netcdf(out_file_p)
out_file_p
