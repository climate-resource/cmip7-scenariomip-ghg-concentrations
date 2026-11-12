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
# # Input emissions
#
# Exploring the emissions files

# %%
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from glob import glob

# %% tags=["parameters"]
emissions_file: str = (
    "../../data/raw/input-scenarios/"
    "202603251220_202512071232_202511040855_202511040855_complete-emissions.csv"
)
zoom_start_year: int = 2015
zoom_end_year: int = 2300

# (model, scenario, cmip7 experiment name), as in scripts/generate-concentration-files.py
markers = (
    ("REMIND-MAgPIE 3.5-4.11", "SSP1 - Very Low Emissions", "vl"),
    ("AIM 3.0", "SSP2 - Low Overshoot_a", "ln"),
    ("MESSAGEix-GLOBIOM-GAINS 2.1-M-R12", "SSP2 - Low Emissions", "l"),
    ("COFFEE 1.6", "SSP2 - Medium-Low Emissions", "ml"),
    ("IMAGE 3.4", "SSP2 - Medium Emissions", "m"),
    ("WITCH 6.0", "SSP5 - Medium-Low Emissions_a", "hl"),
    ("GCAM 8s", "SSP3 - High Emissions", "h"),
)
palette = {
    "vl": "#24a4ff",
    "ln": "#4a0daf",
    "l": "#00cc69",
    "ml": "#f5ac00",
    "m": "#ffa9dc",
    "h": "#700000",
    "hl": "#8f003b",
}

# %%
glob('../../data/raw/input-scenarios/*')

# %%

# %% [markdown]
# ## Load

# %%
raw = pd.read_csv(Path(emissions_file))
year_cols = [c for c in raw.columns if c.isdigit()]
raw = raw.rename(columns={c: int(c) for c in year_cols})
years = [int(c) for c in year_cols]

variable = "Emissions|CH4"
world = raw[(raw["region"] == "World") & (raw["variable"].isin([variable]))]
sorted(world["variable"].unique())

# %% [markdown]
# ## Plot

# %%
fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(16, 5))
unit = None
for model, scenario, code in markers:
    row = world[(world["model"] == model) & (world["scenario"] == scenario) & (world["variable"] == variable)]
    if row.empty:
        print(f"No {variable} for {model} / {scenario}")
        continue
    unit = row["unit"].iloc[0]
    series = row[years].iloc[0]
    for ax in axes:
        ax.plot(series.index, series.values, color=palette[code], label=f"{code} ({scenario})")
axes[1].set_xlim(zoom_start_year, min(zoom_end_year, max(years)))
axes[0].set_title(variable)
axes[1].set_title(f"{variable} - {zoom_start_year} to {zoom_end_year}")
for ax in axes:
    ax.set_xlabel("Year")
    ax.set_ylabel(unit)
    ax.axhline(0.0, linestyle="--", color="gray")
    ax.grid()
axes[0].legend()
plt.tight_layout()
plt.show()

# %%
