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
# # Compare `baseline-cmip7-1.1.0` (A) vs `esgf-v1.1.0` (B)
#
# To compare something else: copy this notebook,
# change the paths in the next cell, then run all cells.

# %%
import diff_tools as dt
import matplotlib.pyplot as plt
import pandas as pd

# %% [markdown]
# ## What to compare

# %%
BUNDLE_A = dt.REPO_ROOT / "output-bundles" / "baseline-cmip7-1.1.0"
BUNDLE_B = dt.REPO_ROOT / "output-bundles" / "esgf-v1.1.0"

# If one side is a mirror of ESGF data, it is downloaded here first (missing files only).
# Set to `None` when comparing two local runs/folders.
DOWNLOAD_ESGF_INTO = BUNDLE_B
ESGF_SOURCE_IDS = [f"CR-{scenario}{ext}-1-1-0" for scenario in dt.SCENARIO_ORDER for ext in ("", "-ext")]

# Set to e.g. ["ccl4"] to only compare some gases
GASES = None

# %% [markdown]
# ## What counts as a difference

# %%
NOISE_RULE = dt.NoiseRule(float32_steps=8, abs_floor_fraction=1e-6)
IGNORED_ATTRS = dt.DEFAULT_IGNORED_ATTRS

print(NOISE_RULE.describe())
print(f"\nAttributes not compared: {', '.join(IGNORED_ATTRS)}")

# %% [markdown]
# ## Download ESGF data (if needed)

# %%
if DOWNLOAD_ESGF_INTO is not None:
    display(dt.download_esgf_files(ESGF_SOURCE_IDS, DOWNLOAD_ESGF_INTO))  # noqa: F821

# %% [markdown]
# ## Compare

# %%
result = dt.compare_bundles(BUNDLE_A, BUNDLE_B, noise_rule=NOISE_RULE, ignored_attrs=IGNORED_ATTRS, gases=GASES)

# %% [markdown]
# ## Verdict

# %%
dt.print_verdict(result)

# %% [markdown]
# ## Files, structure and metadata
#
# Files present on only one side,
# files whose variables/dimensions/coordinates differ,
# and attributes that differ (other than the ignored ones).

# %%
print(f"Only in A ({len(result.only_in_a)}):", *result.only_in_a[:50], sep="\n  ")
print(f"Only in B ({len(result.only_in_b)}):", *result.only_in_b[:50], sep="\n  ")

# %%
result.files.loc[result.files["status"] == dt.STATUS_STRUCTURE, ["file", "structure_issues"]]

# %%
if result.attribute_differences.empty:
    print("No attribute differences")
else:
    display(  # noqa: F821
        result.attribute_differences.groupby(["where", "attribute"])
        .agg(n_files=("file", "count"), example_a=("value_a", "first"), example_b=("value_b", "first"))
        .sort_values("n_files", ascending=False)
    )

# %% [markdown]
# ## Differences per gas

# %%
with pd.option_context("display.max_rows", None, "display.float_format", "{:.3g}".format):
    display(dt.summarise(result, by=["variable"]))  # noqa: F821

# %% [markdown]
# ### Gases with differences, by scenario and period

# %%
by_period = dt.summarise(result, by=["variable", "scenario", "period"])
n_files = by_period[list(dt.STATUSES)].sum(axis=1)
not_identical = by_period[by_period[dt.STATUS_IDENTICAL] < n_files]
with pd.option_context("display.max_rows", None, "display.float_format", "{:.3g}".format):
    display(not_identical)  # noqa: F821

# %% [markdown]
# ## Difference plots
#
# Only gases with at least one difference above noise are plotted.
# Lines: global mean, A - B, with the noise band in grey.
# Heatmaps: zonal grids, A - B, differences within noise are shown as white.

# %%
to_plot = dt.gases_above_noise(result)
print(f"Gases with differences above noise: {to_plot}")

# %%
for gas in to_plot:
    fig = dt.plot_gas_differences(result, gas, frequency="mon")
    plt.show()

# %%
