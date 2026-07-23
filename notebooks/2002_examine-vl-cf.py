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

# %% [markdown]
# # Examine vl-cf output
#
# Should match history plus vl for many gases,
# but differ from 2016-01 onwards for others.

# %%
import json
import re
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pooch
import requests
import tqdm.auto
import xarray as xr

# %%
# Double check these values before running
output_bundle = "1.1.0"
output_bundle = "dev-test"
local_out_root = Path(f"../output-bundles/{output_bundle}/data/processed/esgf-ready/")
compare_to_esgf_version = "1.1.0"
compare_to_esgf_version_history_source_id = "CR-CMIP-1-0-0"


# %%
def get_esgf_url(local_fp: Path) -> str:
    """
    Get ESGF HTTP download URL from a filepath
    """
    url_index = "https://esgf-node.ornl.gov/esgf-1-5-bridge"

    session = requests.Session()
    retries = requests.adapters.Retry(
        # Just in case flaky
        total=10,
        backoff_factor=0.1,
        status_forcelist=[401, 429],
    )
    session.mount("https://", requests.adapters.HTTPAdapter(max_retries=retries))

    params = dict(
        replica=False,
        activity_id="input4MIPs",
        type="File",
        limit=10,
        # facets="source_id",
        variable_id=local_fp.parts[-4],
        source_id=local_fp.parts[-7].replace(
            output_bundle.replace(".", "-"), compare_to_esgf_version.replace(".", "-")
        ),
        frequency=local_fp.parts[-5],
        grid_label=local_fp.parts[-3],
        data_node="esgf-node.ornl.gov",
    )
    r = session.get(url_index, params=params)
    r_json = r.json()

    if r_json["response"]["numFound"] == 0:
        msg = f"No ESGF results for {local_fp}. {params=} {r_json=}"
        raise AssertionError(msg)

    if r_json["response"]["numFound"] > 1:
        id_match = ".".join(local_fp.parts[-11:])
        matching = [
            v
            for v in r_json["response"]["docs"]
            if re.sub(r"v\d*", "v", v["id"].split("|")[0]) == re.sub(r"v\d*", "v", id_match)
        ]
        if len(matching) != 1:
            raise AssertionError

        r_json["response"]["docs"] = matching

    record = r_json["response"]["docs"][0]
    urls = record["url"]
    checksum = record["checksum"][0]

    for url in urls:
        if url.endswith("netcdf|HTTPServer"):
            url_http = url.split("|")[0]
            break
    else:
        msg = f"Did not find netCDF HTTP URL: {urls=}"
        raise AssertionError(msg)

    return url_http, checksum


# %%
try:
    with open("esgf-url-checksums.json") as fh:
        esgf_url_checksums = json.load(fh)
except FileNotFoundError:
    esgf_url_checksums = {}

# %%
local_vl_cf_fps = list(local_out_root.glob("input4MIPs/**/*CR-vl-cf*.nc"))
# local_vl_cf_fps

# %%
gases = set(v.parts[-4] for v in local_vl_cf_fps)
assert len(gases) == 46, len(gases)

# %%
grids = set(v.parts[-3] for v in local_vl_cf_fps)
assert len(grids) == 3, grids
grids

# %%
frequencies = set(v.parts[-5] for v in local_vl_cf_fps)
assert len(frequencies) == 2, frequencies
frequencies

# %%
for gas in tqdm.auto.tqdm(sorted(gases), desc="Gases"):
    for grid, frequency, plot in tqdm.auto.tqdm(
        (
            ("gm", "yr", True),
            # ("gm", "mon", True),
            # ("gnz", "mon", False),
            # ("gr1z", "yr", False),
            # ("gr1z", "mon", False),
        ),
        desc="grids and frequencies",
        leave=False,
    ):
        local_vl_cf_fps_gas = [
            v for v in local_vl_cf_fps if v.parts[-4] == gas and v.parts[-3] == grid and v.parts[-5] == frequency
        ]

        local_vl_cf_ds = (
            xr.open_mfdataset([v for v in local_vl_cf_fps_gas if "ext" not in v.name], use_cftime=True)
            .assign_coords({"scenario": "vl-cf"})
            .expand_dims("scenario")
        )
        local_vl_cf_ext_ds = (
            xr.open_mfdataset([v for v in local_vl_cf_fps_gas if "ext" in v.name], use_cftime=True)
            .assign_coords({"scenario": "vl-cf-ext"})
            .expand_dims("scenario")
        )
        # break
        esgf_files_gas = []
        for fp in tqdm.auto.tqdm(local_vl_cf_fps_gas):
            fp_to_get = Path(
                "/".join(
                    v.replace("vl-cf", "vl")
                    .replace("PolMIP", "ScenarioMIP")
                    .replace("2016-2100", "1750-2022")
                    .replace("201601-210012", "175001-202212")
                    for v in fp.parts
                )
            )
            if fp_to_get.name in esgf_url_checksums:
                url, checksum = esgf_url_checksums[fp_to_get.name]
            else:
                url, checksum = get_esgf_url(fp_to_get)
                esgf_url_checksums[fp_to_get.name] = (url, checksum)

            esgf_file_vl = pooch.retrieve(url, known_hash=checksum)
            esgf_files_gas.append(esgf_file_vl)
            if "ext" not in fp.name:
                history_fp = Path(
                    "/".join(
                        v.replace("PolMIP", "CMIP")
                        .replace("2016-2100", "1750-2022")
                        .replace("201601-210012", "175001-202212")
                        .replace(fp.parts[-7], compare_to_esgf_version_history_source_id)
                        for v in fp.parts
                    )
                )
                if history_fp.name in esgf_url_checksums:
                    url, checksum = esgf_url_checksums[history_fp.name]
                else:
                    url, checksum = get_esgf_url(history_fp)
                    esgf_url_checksums[history_fp.name] = (url, checksum)

                esgf_file_history = pooch.retrieve(url, known_hash=checksum)
                esgf_files_gas.append(esgf_file_history)

        esgf_files_gas = [Path(v) for v in esgf_files_gas]

        local_history_ds = (
            xr.open_mfdataset(
                [v for v in esgf_files_gas if "ext" not in v.name and "vl" not in v.name], use_cftime=True
            )
            .assign_coords({"scenario": "hist-vl"})
            .expand_dims("scenario")
        )
        local_vl_ds = (
            xr.open_mfdataset([v for v in esgf_files_gas if "ext" not in v.name and "vl" in v.name], use_cftime=True)
            .assign_coords({"scenario": "hist-vl"})
            .expand_dims("scenario")
        )
        local_historical_vl_ds = xr.concat(
            [local_history_ds, local_vl_ds.sel(time=local_vl_ds["time"].dt.year > 2022)], dim="time"
        )
        local_vl_ext_ds = (
            xr.open_mfdataset([v for v in esgf_files_gas if "ext" in v.name], use_cftime=True)
            .assign_coords({"scenario": "vl-ext"})
            .expand_dims("scenario")
        )
        # local_vl_ext_ds
        if plot:
            plot_dss = [
                local_vl_cf_ds,
                local_vl_cf_ext_ds,
                local_historical_vl_ds,
                local_vl_ext_ds,
            ]

            fig, axes = plt.subplots(ncols=3, figsize=(12, 4))
            markers = ["+", "x", "o", "^"]

            for ax, xlim in zip(axes, ((2000, 2500), (2010, 2040), (2090, 2110))):
                for i, plot_ds in enumerate(plot_dss):
                    if frequency == "yr":
                        time_axis = plot_ds["time"].dt.year.values.squeeze()

                    elif frequency == "mon":
                        time_axis = (plot_ds["time"].dt.year + (plot_ds["time"].dt.month + 1) / 24).values.squeeze()

                    else:
                        raise NotImplementedError(frequency)

                    ax.plot(
                        time_axis,
                        plot_ds[gas].values.squeeze(),
                        label=str(plot_ds["scenario"].values[0]),
                        alpha=0.7,
                        linewidth=3,
                        marker=markers[i],
                        markersize=8,
                    )
                    # plot_ds[gas].plot.line(x="time", hue="scenario")
                    # break

                ax.legend()
                ax.grid()

                if frequency in ("yr", "mon"):
                    ax.set_xlim(xlim)

                else:
                    raise NotImplementedError(frequency)

            plt.suptitle(f"{gas} {frequency} {grid}")
            plt.tight_layout()
            plt.show()

        if gas not in {
            "co2",
            "cfc11eq",
            "ch4",
            "hfc134a",
            "hfc134aeq",
            "hfc152a",
            "hfc23",
            "hfc245fa",
            "hfc32",
            "hfc365mfc",
            "n2o",
        }:
            vl_cf_min_year = local_vl_cf_ds["time"].dt.year.min()
            np.testing.assert_allclose(
                local_vl_cf_ds[gas].values,
                local_historical_vl_ds[gas].sel(time=local_historical_vl_ds["time"].dt.year >= vl_cf_min_year).values,
                rtol=1e-4,
            )
            np.testing.assert_allclose(
                local_vl_cf_ext_ds[gas].values,
                local_vl_ext_ds[gas].values,
                rtol=1e-4,
            )

# %%
with open("esgf-url-checksums.json", "w") as fh:
    json.dump(esgf_url_checksums, fh)
