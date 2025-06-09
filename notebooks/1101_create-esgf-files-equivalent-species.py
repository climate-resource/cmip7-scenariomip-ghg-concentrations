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
# # Create files for ESGF - equivalent species

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Imports

# %% editable=true slideshow={"slide_type": ""}
import itertools
from functools import partial
from pathlib import Path

import cftime
import matplotlib.pyplot as plt
import numpy as np
import openscm_units
import pandas as pd
import pandas_indexing as pix  # noqa: F401
import pandas_openscm
import pint_xarray
import tqdm.auto
import xarray as xr
from attrs import evolve
from input4mips_validation.cvs.loading import load_cvs_known_loader
from input4mips_validation.cvs.loading_raw import get_raw_cvs_loader
from input4mips_validation.dataset import Input4MIPsDataset
from input4mips_validation.dataset.dataset import prepare_ds_and_get_frequency
from input4mips_validation.dataset.metadata_data_producer_minimum import (
    Input4MIPsDatasetMetadataDataProducerMinimum,
)
from input4mips_validation.xarray_helpers import add_time_bounds

from cmip7_scenariomip_ghg_generation.constants import GHG_RADIATIVE_EFFICIENCIES, VARIABLE_TO_STANDARD_NAME_RENAMING
from cmip7_scenariomip_ghg_generation.xarray_helpers import (
    calculate_cos_lat_weighted_mean_latitude_only,
    convert_year_to_time,
)

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Parameters

# %% editable=true slideshow={"slide_type": ""} tags=["parameters"]
equivalent_species: str = "cfc12eq"
components = "cfc11;;cfc113;;cfc114;;cfc115;;cfc12;;ccl4;;ch2cl2;;ch3br;;ch3ccl3;;ch3cl;;chcl3;;halon1211;;halon1301;;halon2402;;hcfc141b;;hcfc142b;;hcfc22"  # noqa: E501
cmip_scenario_name: str = "vllo"
input4mips_cvs_source: str = "gh:cr-scenariomip"
esgf_ready_root_dir: str = "../output-bundles/dev-test/data/processed/esgf-ready"


# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Parse parameters

# %% editable=true slideshow={"slide_type": ""}
components_p = tuple(components.split(";;"))
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
def load_component_da(component: str) -> xr.DataArray:
    """
    Load data for a given component
    """
    # Start from 15-degree data
    candidates = list(esgf_ready_root_dir_p.rglob(f"**/{component}/gnz/**/*-{cmip_scenario_name}-*.nc"))
    if len(candidates) != 1:
        msg = f"{component=} {candidates=}"
        raise AssertionError(msg)

    res = xr.open_dataset(candidates[0])[component]
    # Pop out lat units to avoid pint quantification issues
    res["lat"].attrs.pop("units")
    res = res.pint.quantify(unit_registry=ur)

    return res


# %%
component_das = {ghg: load_component_da(ghg) for ghg in tqdm.auto.tqdm(components_p)}
# components

# %% [markdown]
# ## Create 15-degree grid equivalent product


# %%
def get_erf(da: xr.DataArray) -> xr.DataArray:
    """Get ERF equivalent for a given spcies"""
    return da * GHG_RADIATIVE_EFFICIENCIES[da.name]


# %%
equiv_erf_df_l = []

equiv_erf = get_erf(component_das[components_p[0]])
for component in components_p:
    component_erf = get_erf(component_das[component])
    if not equiv_erf_df_l:
        equiv_erf = component_erf

    else:
        equiv_erf = equiv_erf + component_erf

    equiv_erf_df_l.append(
        component_erf.groupby("time.year")
        .mean()
        .mean("lat")
        .pint.to("W / m^2")
        .pint.dequantify()
        .to_dataframe()[component]
    )

# equiv_erf
equiv_erf_df = pd.concat(equiv_erf_df_l, axis="columns")
equiv_erf_df = equiv_erf_df.T.sort_values(by=2023, ascending=False).T

ax = equiv_erf_df.plot.area()
ax.legend(loc="center left", bbox_to_anchor=(1.05, 0.5))
plt.show()

tmp = equiv_erf_df.sum(axis="columns")
tmp.name = "total"
equiv_erf_df = pd.concat([equiv_erf_df, tmp], axis="columns")

ax = equiv_erf_df.plot()
ax.legend(loc="center left", bbox_to_anchor=(1.05, 0.5))
plt.show()

# %%
native_grid = equiv_erf / GHG_RADIATIVE_EFFICIENCIES[equivalent_species.replace("eq", "")]
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
# global_mean

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
y_to_time = partial(
    convert_year_to_time,
    month=7,
    day=2,
    hour=12,
    calendar="proleptic_gregorian",
)


# %%
def get_displayable_dataarray(inp: xr.DataArray) -> xr.DataArray:
    """
    Get a :obj:`xr.DataArray` which we can dispaly

    There is some bug in xarray's HTML representation which
    means this doesn't work with a proleptic_gregorian calendar.
    """
    res = inp.copy()
    res["time"] = np.array(
        [cftime.datetime(v.year, v.month, v.day, v.hour, calendar="standard") for v in inp["time"].values]
    )

    return res


# %%
global_mean_annual_mean = y_to_time(global_mean.groupby("time.year").mean())
hemispheric_means_annual_mean = y_to_time(hemispheric_means.groupby("time.year").mean())
# get_displayable_dataarray(global_mean_annual_mean)
# get_displayable_dataarray(hemispheric_means_annual_mean)

# %%
print("Annual-means")
global_mean_annual_mean.plot()
plt.show()

hemispheric_means_annual_mean.plot(hue="lat")
plt.show()

# %% [markdown]
# ## Write to ESGF-ready

# %% [markdown]
# ### Set common metadata

# %%
metadata_helper = xr.open_mfdataset(
    esgf_ready_root_dir_p.rglob(f"**/{equivalent_species.replace('eq', '')}/gnz/**/*-{cmip_scenario_name}-*.nc")
)
# metadata_helper

# %%
source_id = metadata_helper.attrs["source_id"]
source_id

# %%
doi = metadata_helper.attrs["doi"]
doi

# %%
metadata_minimum_common = dict(
    source_id=source_id,
    target_mip="ScenarioMIP",
)
metadata_minimum_common

# %%
comment = (
    f"{equivalent_species} is the equivalent of {', '.join(sorted(components_p))}. "
    "Data compiled by Climate Resource, based on science by many others "
    "(see 'references*' attributes). "
    "For funding information, see the 'funding*' attributes."
)
# comment

# %%
non_input4mips_metadata_common = {
    k: metadata_helper.attrs[k]
    # TODO: be more careful with references
    for k in (
        "references",
        "references_short_names",
        "references_dois",
        "references_urls",
        "funding",
        "funding_short_names",
        "funding_urls",
    )
}

# %% [markdown]
# ### Grab the CVs

# %%
raw_cvs_loader = get_raw_cvs_loader(
    input4mips_cvs_source,
    # # Can force updates if you need using this
    # force_download=True,
)
# raw_cvs_loader

# %%
cvs = load_cvs_known_loader(raw_cvs_loader)
if source_id not in cvs.source_id_entries.source_ids:
    raise AssertionError(source_id)

# %% [markdown]
# ### Set up time ranges
#
# We tend to not write all data in a single file,
# rather in chunks to make for easier handling.

# %%
time_dimension = "time"

# %%
# Extensions will be a different thing hence hard-code end for now
time_ranges_to_write = [range(int(global_mean_annual_mean[time_dimension].dt.year[0]), 2100 + 1)]
# time_ranges_to_write = [range(1750, int(global_mean_annual_mean.time.dt.year[-1].values) + 1)]

for start, end in itertools.pairwise(time_ranges_to_write):
    assert start[-1] == end[0] - 1

time_ranges_to_write

# %% [markdown]
# ### Get standard name
#
# These are from [this list](https://cfconventions.org/Data/cf-standard-names/current/build/cf-standard-name-table.html)
# or made up to look as close as possible.

# %%
standard_name = VARIABLE_TO_STANDARD_NAME_RENAMING[equivalent_species]
standard_name

# %% [markdown]
# ### Write files

# %%
# papermill_description=write-files
esgf_ready_root_dir_p.mkdir(exist_ok=True, parents=True)
for dat_resolution, grid_label, nominal_resolution, yearly_time_bounds in tqdm.auto.tqdm(
    [
        (native_grid, "gnz", "2500 km", False),
        # # (half_degree_data, "05_deg_lat", False),
        (global_mean, "gm", "10000 km", False),
        (global_mean_annual_mean, "gm", "10000 km", True),
        (hemispheric_means, "gr1z", "10000 km", False),
        (hemispheric_means_annual_mean, "gr1z", "10000 km", True),
    ],
    desc="Resolutions",
):
    # TODO: calculate nominal resolution rather than guessing
    grid_info = " x ".join([f"{dat_resolution[v].size} ({v})" for v in dat_resolution.dims])
    print(f"Processing {grid_info} grid")

    variable_name_raw = equivalent_species
    variable_name_output = equivalent_species

    ds_to_write = dat_resolution.to_dataset(name=variable_name_output).pint.dequantify()

    dimensions = tuple(str(v) for v in ds_to_write[variable_name_output].dims)
    print(f"{grid_label=}")
    print(f"{dimensions=}")

    # Use appropriate precision
    ds_to_write[variable_name_output] = ds_to_write[variable_name_output].astype(np.dtypes.Float32DType)
    ds_to_write["time"].encoding = {
        "calendar": "proleptic_gregorian",
        "units": "days since 1850-01-01",
        # Time has to be encoded as float
        # to ensure that non-integer days etc. can be handled
        # and the CF-checker doesn't complain.
        "dtype": np.dtypes.Float32DType,
    }

    if "lat" in dimensions:
        ds_to_write["lat"].encoding = {"dtype": np.dtypes.Float16DType}

    metadata_minimum = Input4MIPsDatasetMetadataDataProducerMinimum(
        grid_label=grid_label,
        nominal_resolution=nominal_resolution,
        **metadata_minimum_common,
    )

    for time_range in time_ranges_to_write:
        ds_to_write_time_section = ds_to_write.sel(time=ds_to_write.time.dt.year.isin(time_range))

        input4mips_ds = Input4MIPsDataset.from_data_producer_minimum_information(
            data=ds_to_write_time_section,
            prepare_func=partial(
                prepare_ds_and_get_frequency,
                dimensions=dimensions,
                time_dimension=time_dimension,
                standard_and_or_long_names={
                    variable_name_output: {
                        "standard_name": standard_name,
                        "long_name": variable_name_raw,
                    },
                },
                add_time_bounds=partial(
                    add_time_bounds,
                    monthly_time_bounds=not yearly_time_bounds,
                    yearly_time_bounds=yearly_time_bounds,
                ),
            ),
            metadata_minimum=metadata_minimum,
            cvs=cvs,
            dataset_category="GHGConcentrations",
            realm="atmos",
        )

        metadata_evolved = evolve(
            input4mips_ds.metadata,
            product="derived",
            comment=comment,
            doi=doi,
        )

        ds = input4mips_ds.data
        ds[variable_name_output].attrs["cell_methods"] = "area: time: mean"
        input4mips_ds = Input4MIPsDataset(
            data=ds,
            metadata=metadata_evolved,
            cvs=cvs,
            non_input4mips_metadata=non_input4mips_metadata_common,
        )

        print("Writing")
        written = input4mips_ds.write(esgf_ready_root_dir_p)
        print(f"Wrote: {written.relative_to(esgf_ready_root_dir_p)}")

    print("")
