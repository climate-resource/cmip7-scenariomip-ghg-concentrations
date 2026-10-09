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
# # Archive an input set
#
# Builds (or updates) one named entry under `input-sets/` with everything
# needed to later "activate" it into a scenariomip run:
#
# - `historical-ghg-concs/`: exact-path copies of the files
#   `download_cmip7_historical_ghg_concentrations` would have downloaded
#   (so `pooch` sees them already present and skips the real download).
# - `historical-ghg-data-interim-info.tar.gz`: either a straight copy of an
#   existing tarball (e.g. the real one downloaded from Zenodo), or a
#   rebuilt tarball with specific gases' interim files swapped out for
#   local replacements before re-archiving (needed because `extract_tar`
#   unconditionally overwrites on extraction, so loose substituted files
#   would just get clobbered).

# %%
import json
import shutil
import tarfile
from pathlib import Path

# %% tags=["parameters"]
input_set_name: str = "esgf-original"
input_sets_root: str = "../../data/local-fit-comparison/input-sets"

# JSON: {"relative/path/under/historical-ghg-concs.nc": "/absolute/source/file.nc"}
historical_concs_file_map_json: str = "{}"

# Path to an existing tarball to use as the interim archive's base.
historical_interim_base_tarball: str = ""

# If empty, historical_interim_base_tarball is just copied as-is.
# If non-empty, JSON: {"<gas>": "/absolute/source/dir/with/that/gas/interim/files"}
# -- files here overwrite same-named files under
# output-bundles/v1.0.0/data/interim/<gas>/ inside the base tarball before
# re-archiving.
historical_interim_replacement_dirs_json: str = "{}"

# %%
input_set_dir = Path(input_sets_root) / input_set_name
concs_dir = input_set_dir / "historical-ghg-concs"
concs_dir.mkdir(parents=True, exist_ok=True)

historical_concs_file_map = json.loads(historical_concs_file_map_json)
for rel_target, abs_source in historical_concs_file_map.items():
    abs_source_p = Path(abs_source)
    target_p = concs_dir / rel_target
    target_p.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(abs_source_p, target_p)
    print(f"Copied {abs_source_p} -> {target_p}")

# %%
historical_interim_replacement_dirs = json.loads(historical_interim_replacement_dirs_json)
tarball_out = input_set_dir / "historical-ghg-data-interim-info.tar.gz"

if not historical_interim_base_tarball:
    print("No base tarball given, skipping historical-ghg-data-interim-info.tar.gz")

elif not historical_interim_replacement_dirs:
    shutil.copy2(historical_interim_base_tarball, tarball_out)
    print(f"Copied base tarball as-is: {historical_interim_base_tarball} -> {tarball_out}")

else:
    staging_dir = input_set_dir / "_staging-interim-tarball"
    if staging_dir.exists():
        shutil.rmtree(staging_dir)
    staging_dir.mkdir(parents=True)

    with tarfile.open(historical_interim_base_tarball, "r:gz") as tar:
        tar.extractall(staging_dir)  # noqa: S202 # trusted, previously downloaded by us

    interim_root = staging_dir / "output-bundles" / "v1.0.0" / "data" / "interim"
    for gas, source_dir in historical_interim_replacement_dirs.items():
        gas_dir = interim_root / gas
        gas_dir.mkdir(parents=True, exist_ok=True)
        for f in Path(source_dir).glob("*"):
            if f.is_file():
                shutil.copy2(f, gas_dir / f.name)
                print(f"Substituted {f} -> {gas_dir / f.name}")

    with tarfile.open(tarball_out, "w:gz") as tar:
        tar.add(staging_dir / "output-bundles", arcname="output-bundles")

    shutil.rmtree(staging_dir)
    print(f"Rebuilt tarball with substitutions: {tarball_out}")

# %%
print(f"Input set '{input_set_name}' archived at {input_set_dir.resolve()}")
