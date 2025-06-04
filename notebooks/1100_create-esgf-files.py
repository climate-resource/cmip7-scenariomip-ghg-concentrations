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
# # Create files for ESGF

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
import pandas_indexing as pix  # noqa: F401
import pandas_openscm
import pint_xarray
import tqdm.auto
import xarray as xr
from attrs import evolve
from input4mips_validation.cli import validate_tree
from input4mips_validation.cvs.loading import load_cvs_known_loader
from input4mips_validation.cvs.loading_raw import get_raw_cvs_loader
from input4mips_validation.dataset import Input4MIPsDataset
from input4mips_validation.dataset.dataset import prepare_ds_and_get_frequency
from input4mips_validation.dataset.metadata_data_producer_minimum import (
    Input4MIPsDatasetMetadataDataProducerMinimum,
)
from input4mips_validation.inference.from_data import (
    BoundsInfo,
    FrequencyMetadataKeys,
)
from input4mips_validation.xarray_helpers import add_time_bounds
from input4mips_validation.xarray_helpers.variables import (
    XRVariableHelper,
)

from cmip7_scenariomip_ghg_generation.constants import VARIABLE_TO_STANDARD_NAME_RENAMING
from cmip7_scenariomip_ghg_generation.xarray_helpers import (
    calculate_cos_lat_weighted_mean_latitude_only,
    convert_time_to_year_month,
    convert_year_month_to_time,
    convert_year_to_time,
)

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Parameters

# %% editable=true slideshow={"slide_type": ""} tags=["parameters"]
esgf_version: str = "0.0.1"
esgf_institution_id: str = "CR"
input4mips_cvs_source: str = "gh:cr-scenariomip"
doi: str = "dev-test-doi"
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
esgf_ready_root_dir: str = "../output-bundles/dev-test/data/processed/esgf-ready"


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
lda = partial(xr.load_dataarray, use_cftime=True)
lds = partial(xr.load_dataset, use_cftime=True)

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ### Global-mean monthly

# %%
global_mean_monthly_no_seasonality = lda(global_mean_monthly_file_p).pint.quantify(unit_registry=ur)
# global_mean_monthly_no_seasonality

# %% [markdown]
# ### Seasonality

# %%
seasonality_month_year = lda(seasonality_file_p).pint.quantify(unit_registry=ur)
seasonality = convert_year_month_to_time(seasonality_month_year)
# seasonality

# %% [markdown]
# ### Latitudinal gradient info

# %%
lat_grad_info = lds(lat_gradient_file_p).pint.quantify(unit_registry=ur)
# lat_grad_info

# %% [markdown]
# ## Create 15-degree grid product

# %% [markdown]
# ### Crunch latitudinal gradient

# %%
lat_grad = (lat_grad_info["eofs"] * lat_grad_info["principal-components-monthly"]).sum("eof")
# lat_grad

# %% [markdown]
# ### Combine

# %%
global_mean_monthly_ym = convert_time_to_year_month(global_mean_monthly_no_seasonality)
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
np.testing.assert_allclose(seasonality_ym.mean("month").data.m, 0.0, atol=1e-6)

# %%
np.testing.assert_allclose(calculate_cos_lat_weighted_mean_latitude_only(lat_grad_ym).data.m, 0.0, atol=1e-8)

# %%
native_grid_ym = global_mean_monthly_ym + seasonality_ym + lat_grad_ym
# native_grid_ym

# %%
ym_to_time = partial(convert_year_month_to_time, day=15)

# %%
native_grid = ym_to_time(native_grid_ym)
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
source_id = f"{esgf_institution_id}-{cmip_scenario_name}-{esgf_version.replace('.', '-')}"
source_id

# %%
metadata_minimum_common = dict(
    source_id=source_id,
    target_mip="ScenarioMIP",
)
metadata_minimum_common

# %%
funding_info = (
    {
        "name": "GHG Forcing For CMIP",
        "url": "climate.esa.int/supporting-modelling/cmip-forcing-ghg-concentrations/",
        "long_text": (
            "This research has been funded by the European Space Agency (ESA) as part of the "
            "GHG Forcing For CMIP project of the Climate Change Initiative (CCI) "
            "(ESA Contract No. 4000146681/24/I-LR-cl)."
        ),
    },
)
comment = (
    "Data compiled by Climate Resource, based on science by many others "
    "(see 'references*' attributes). "
    "For funding information, see the 'funding*' attributes."
)

# %%
# TODO: handle this better
# Probably just pass in short names
# from scripts and then just load the rest from a hard-coded DB
gas_deps = (
    dict(
        short_name="Nicholls et al., 2025 (in-prep)",
        licence="Paper, NA",
        reference=(
            "Nicholls, Z., Meinshausen, M., Lewis, J., Pflueger, M., Menking, A., ...: "
            "Greenhouse gas concentrations for climate modelling (CMIP7), "
            "in-prep, 2025."
        ),
        url="https://github.com/climate-resource/CMIP-GHG-Concentration-Generation",
        # resource_type="publication-article",
    ),
    dict(
        short_name="Nicholls et al., 2025 (in-prep)",
        licence="Paper, NA",
        reference=(
            "Nicholls, Z., Meinshausen, M., Lewis, J., Pflueger, M., Menking, A., ...: "
            "Future greenhouse gas concentrations for climate modelling (CMIP7 ScenarioMIP), "
            "in-prep, 2025."
        ),
        url="https://github.com/climate-resource/cmip7-scenariomip-ghg-concentrations",
        # resource_type="publication-article",
    ),
    dict(
        short_name="Meinshausen et al., 2020",
        licence="Paper, NA",
        reference=(
            "Meinshausen, M., Nicholls, Z. R. J., ..., Vollmer, M. K., and Wang, R. H. J.: "
            "The shared socio-economic pathway (SSP) greenhouse gas concentrations "
            "and their extensions to 2500, "
            "Geosci. Model Dev., 13, 3571-3605, https://doi.org/10.5194/gmd-13-3571-2020, 2020."
        ),
        doi="https://doi.org/10.5194/gmd-13-3571-2020",
        url="https://doi.org/10.5194/gmd-13-3571-2020",
        # resource_type="publication-article",
    ),
)

# %%
non_input4mips_metadata_common = {
    "references": " --- ".join([v["reference"] for v in gas_deps]),
    "references_short_names": " --- ".join([v["short_name"] for v in gas_deps]),
    "references_dois": " --- ".join(
        [v["doi"] if ("doi" in v and v["doi"] is not None) else "No DOI" for v in gas_deps]
    ),
    "references_urls": " --- ".join([v["url"] for v in gas_deps]),
    "funding": " ".join([v["long_text"] for v in funding_info]),
    "funding_short_names": " --- ".join([v["name"] for v in funding_info]),
    "funding_urls": " --- ".join([v["url"] for v in funding_info]),
}
non_input4mips_metadata_common

# %% [markdown]
# ### Grab the CVs

# %%
# Force downloads while we are experimenting
raw_cvs_loader = get_raw_cvs_loader(input4mips_cvs_source, force_download=True)
# raw_cvs_loader

# %%
cvs = load_cvs_known_loader(raw_cvs_loader)
if source_id not in cvs.source_id_entries.source_ids:
    raise AssertionError

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
standard_name = VARIABLE_TO_STANDARD_NAME_RENAMING[ghg]
standard_name

# %% [markdown]
# ### Write files

# %%
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

    variable_name_raw = ghg
    variable_name_output = ghg

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

# %% [markdown]
# ## Validate the written files

# %%
bounds_info = BoundsInfo(
    time_bounds="time_bnds",
    bounds_dim="bnds",
    bounds_dim_lower_val=0,
    bounds_dim_upper_val=1,
)
frequency_metadata_keys = FrequencyMetadataKeys(
    frequency_metadata_key="frequency",
    no_time_axis_frequency="fx",
)
xr_variable_processor = XRVariableHelper(
    bounds_coord_indicators=("bounds", "bnds"),
    climatology_bounds_coord_indicators=("climatology",),
)

validate_tree(
    tree_root=esgf_ready_root_dir_p,
    cv_source=input4mips_cvs_source,
    xr_variable_processor=xr_variable_processor,
    frequency_metadata_keys=frequency_metadata_keys,
    bounds_info=bounds_info,
    time_dimension=time_dimension,
    rglob_input=f"**/*{variable_name_output}*/**/*.nc",
    allow_cf_checker_warnings=False,
    output_html=None,
)
