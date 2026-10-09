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
# # Activate an input set for a run
#
# Copies a named `input-sets/<name>/` bundle into a target `--run-id`'s
# `output-bundles/<run-id>/data/raw/` tree, in exactly the paths the
# scenariomip pipeline's download tasks would have written to. Because
# `pooch.retrieve(..., known_hash=None)` treats any existing file at the
# destination as already-fetched, the pipeline will use these copied files
# instead of downloading from ESGF/Zenodo -- no pipeline code changes
# needed.
#
# This is a plain copy, not a link: re-running this notebook for the same
# run-id will just overwrite with the current input-set contents.

# %%
import shutil
from pathlib import Path

# %% tags=["parameters"]
input_set_name: str = "local-fit-satweighted"
run_id: str = "local-fit-satweighted"
input_sets_root: str = "../../data/local-fit-comparison/input-sets"
output_bundles_root: str = "../../output-bundles"

# %%
input_set_dir = Path(input_sets_root) / input_set_name
target_data_raw = Path(output_bundles_root) / run_id / "data" / "raw"
target_data_raw.mkdir(parents=True, exist_ok=True)

# %%
source_concs_dir = input_set_dir / "historical-ghg-concs"
target_concs_dir = target_data_raw / "historical-ghg-concs"

n_copied = 0
for source_file in source_concs_dir.rglob("*"):
    if source_file.is_file():
        rel = source_file.relative_to(source_concs_dir)
        target_file = target_concs_dir / rel
        target_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_file, target_file)
        n_copied += 1

print(f"Copied {n_copied} historical-ghg-concs files into {target_concs_dir}")

# %%
source_tarball = input_set_dir / "historical-ghg-data-interim-info.tar.gz"
target_tarball = target_data_raw / "historical-ghg-data-interim-info.tar.gz"

if source_tarball.exists():
    shutil.copy2(source_tarball, target_tarball)
    print(f"Copied {source_tarball} -> {target_tarball}")
else:
    print(f"No historical-ghg-data-interim-info.tar.gz found in input set {input_set_name}, skipping")

# %%
print(f"Input set '{input_set_name}' activated for run-id '{run_id}'")
