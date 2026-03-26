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
from pathlib import Path

import pooch
import requests
import tqdm.auto
import xarray as xr

# %%
# Double check these values before running
output_bundle = "1.0.1"
local_out_root = Path(f"../output-bundles/{output_bundle}/data/processed/esgf-ready/")
compare_to_esgf_version = "1.0.0"


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

    if r_json["response"]["numFound"] != 1:
        print(f"Be careful, didn't find only one response for {local_fp}")

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
# The extensions means the data is slightly different,
# hence only check equal before a certain period,
# and check close for the entire time period.
compare_equal_before = 2095

checked = []
for fp in tqdm.auto.tqdm(local_out_root.glob("input4MIPs/**/*.nc")):
    if not any(sid in str(fp) for sid in ("-vl-", "-h-")):
        # print(f"Not checking {fp.name} yet as these scenarios haven't been upload to ESGF")
        continue

    if "ext" in str(fp):
        # print(f"Not checking {fp.name} yet as the extensions haven't been upload to ESGF")
        continue

    url, checksum = get_esgf_url(fp)
    esgf_file = pooch.retrieve(url, known_hash=checksum)
    local = xr.load_dataset(fp)
    esgf = xr.load_dataset(esgf_file)

    try:
        xr.testing.assert_allclose(
            local.sel(time=local.time.dt.year < compare_equal_before),
            esgf.sel(time=esgf.time.dt.year < compare_equal_before),
            rtol=1e-8,
            atol=1e-4,
        )
        xr.testing.assert_allclose(
            local,
            esgf,
            rtol=1e-3,
            atol=1e-4,
        )
        checked.append(fp)
    except AssertionError as exc:
        print(f"Issue for {fp=}")
        print(exc)
        raise

print(f"{len(checked)=}")

# %%
checked

# %%
