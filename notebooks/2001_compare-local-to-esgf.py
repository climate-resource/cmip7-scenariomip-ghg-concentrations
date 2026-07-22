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
# # Compare local output to ESGF
#
# This is just a way to check that we can reproduce what is on the ESGF.

# %%
import functools
import json
from pathlib import Path

import numpy as np
import pooch
import requests
import tqdm.auto
import xarray as xr

# %%
# Double check these values before running
output_bundle = "1.1.0"
local_out_root = Path(f"../output-bundles/{output_bundle}/data/processed/esgf-ready/")
compare_to_esgf_version = "1.1.0"


# %%
@functools.cache
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
        variable_id=fp.parts[-4],
        source_id=fp.parts[-7].replace(output_bundle.replace(".", "-"), compare_to_esgf_version.replace(".", "-")),
        frequency=fp.parts[-5],
        grid_label=fp.parts[-3],
        data_node="esgf-node.ornl.gov",
    )
    r = session.get(url_index, params=params)
    r_json = r.json()

    if r_json["response"]["numFound"] == 0:
        msg = f"No ESGF results for {local_fp}. {params=} {r_json=}"
        raise AssertionError(msg)

    if r_json["response"]["numFound"] > 1:
        id_match = ".".join(local_fp.parts[-11:])
        matching = [v for v in r_json["response"]["docs"] if v["id"].split("|")[0] == id_match]
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
# Checks for high.
#
# The extensions means the data is slightly different,
# particularly because of the mean-preserving interpolation,
# hence check closeness with different thresholds for different periods.
compare_closer_before = 2095

to_check = []
for fp in tqdm.auto.tqdm(local_out_root.glob("input4MIPs/**/*.nc")):
    if not any(sid in str(fp) for sid in ("-h-",)):
        continue

    # if not any (ghg in str(fp) for ghg in ("hfc32",)):
    #     continue

    # if not any(sid in str(fp) for sid in ("gm", "gr1z")):
    #     continue

    # if not any(sid in str(fp) for sid in ("yr",)):
    #     continue

    to_check.append(fp)

to_check = sorted(to_check)

checked = []
for fp in tqdm.auto.tqdm(to_check):
    if fp.name in esgf_url_checksums:
        url, checksum = esgf_url_checksums[fp.name]
    else:
        url, checksum = get_esgf_url(fp)
        esgf_url_checksums[fp.name] = (url, checksum)

    esgf_file = pooch.retrieve(url, known_hash=checksum)
    local = xr.load_dataset(fp, use_cftime=True)
    esgf = xr.load_dataset(esgf_file, use_cftime=True)

    ghg = fp.name.split("_")[0]

    try:
        tol_paras = dict(
            # rtol=1e-8,
            atol=1e-3,
        )
        xr.testing.assert_allclose(
            local[ghg].round(3).sel(time=local.time.dt.year < compare_closer_before),
            esgf[ghg].round(3).sel(time=esgf.time.dt.year < compare_closer_before),
            **tol_paras,
        )
        passed_compare_closer_before = True

    except AssertionError as exc:
        passed_compare_closer_before = False
        print(f"Issue before {compare_closer_before} for {fp=}")
        print(exc)

        loc = np.where(~np.isclose(local[ghg].round(3).values, esgf[ghg].round(3).values, **tol_paras))

        print(f"{local[ghg].values[loc]=}")
        print(f"{esgf[ghg].values[loc]=}")
        print(f"{local[ghg].time[loc[0]].values=}")
        print()
        # raise

    try:
        tol_paras = (
            dict(
                # acceptable for the application of interest
                atol=1e-3,
                rtol=1e-3,
            )
            if ghg != "ch2cl2"
            else dict(
                # acceptable for the application of interest.
                # ch2cl2 has crazy short lifetime
                # so the differing extensions
                # (actual extension vs. implicit extension in MAGICC)
                # matter more.
                atol=1e-3,
                rtol=1e-2,
            )
        )
        xr.testing.assert_allclose(local[ghg].round(3), esgf[ghg].round(3), **tol_paras)
        passed_all_time = True

    except AssertionError as exc:
        passed_all_time = False
        print(f"Issue for {fp=}")
        print(exc)

        loc = np.where(~np.isclose(local[ghg].round(3).values, esgf[ghg].round(3).values, **tol_paras))

        print(f"{local[ghg].values[loc]=}")
        print(f"{esgf[ghg].values[loc]=}")
        print(f"{local[ghg].time[loc[0]].values=}")
        print()
        raise

    if passed_compare_closer_before and passed_all_time:
        checked.append(fp)
    # print(f"Checked {fp=}")

print(f"{len(checked)=}")

# %%
# Checks for vl.
# This is different because there was a very small difference
# in input emissions for v1.0.0
# and spruious negative values (my fault).
# Hence the tolerances and checks are different.
#
# The extensions means the data is slightly different,
# particularly because of the mean-preserving interpolation,
# hence check closeness with different thresholds for different periods.
compare_closer_before = 2095

to_check = []
for fp in tqdm.auto.tqdm(local_out_root.glob("input4MIPs/**/*.nc")):
    if not any(sid in str(fp) for sid in ("-vl-",)):
        continue

    # if not any (ghg in str(fp) for ghg in ("hfc32",)):
    #     continue

    # if not any(sid in str(fp) for sid in ("gm", "gr1z")):
    #     continue

    # if not any(sid in str(fp) for sid in ("yr",)):
    #     continue

    to_check.append(fp)

to_check = sorted(to_check)

checked = []
for fp in tqdm.auto.tqdm(to_check):
    if fp.name in esgf_url_checksums:
        url, checksum = esgf_url_checksums[fp.name]
    else:
        url, checksum = get_esgf_url(fp)
        esgf_url_checksums[fp.name] = (url, checksum)

    esgf_file = pooch.retrieve(url, known_hash=checksum)
    local = xr.load_dataset(fp, use_cftime=True)
    esgf = xr.load_dataset(esgf_file, use_cftime=True)

    ghg = fp.name.split("_")[0]

    if ghg == "hfc152a":
        # Relatively big differences due to fixing lat. gradient
        tol_paras = dict(
            rtol=1e-2,
            atol=25e-1,
        )

    elif ghg == "hfc245fa":
        # Relatively big differences due to fixing lat. gradient
        tol_paras = dict(
            rtol=1e-2,
            atol=7e-2,
        )

    elif ghg == "ch2cl2":
        # Super short lifetime
        tol_paras = dict(
            rtol=1e-3,
            atol=2e-3,
        )

    else:
        tol_paras = dict(
            rtol=1e-3,
            atol=1e-8,
        )

    local_ghg = local[ghg]
    esgf_ghg = esgf[ghg]
    diffs_loc = np.where(~np.isclose(local_ghg.values, esgf_ghg.values, **tol_paras))
    if diffs_loc[0].size == 0:
        checked.append(fp)
        continue

    print(f"Issue for {fp=} with {tol_paras=}")
    print(f"{local_ghg.time[diffs_loc[0]].values=}")
    print(f"{local_ghg.values[diffs_loc]=}")
    print(f"{esgf_ghg.values[diffs_loc]=}")

print(f"{len(checked)=}")
# checked

# %%
with open("esgf-url-checksums.json", "w") as fh:
    json.dump(esgf_url_checksums, fh)

# %%
