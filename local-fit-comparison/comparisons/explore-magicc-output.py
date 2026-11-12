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
# # Explore MAGICC output
#
# Opens the MAGICC output database of one run and plots a few variables.

# %%
from pathlib import Path

import matplotlib.pyplot as plt
import pandas_indexing as pix
import pandas_openscm
import pandas_openscm.db

pandas_openscm.register_pandas_accessor()

# %% tags=["parameters"]
run_id: str = "local-fit-satweighted"
output_bundles_root: str = "../../output-bundles"

# %% [markdown]
# ## Open the database

# %%
db_dir = Path(output_bundles_root) / run_id / "data" / "interim" / "magicc-output" / "db"
db = pandas_openscm.db.OpenSCMDB(
    backend_data=pandas_openscm.db.DATA_BACKENDS.get_instance("feather"),
    backend_index=pandas_openscm.db.INDEX_BACKENDS.get_instance("feather"),
    db_dir=db_dir,
)

# %%
metadata = db.load_metadata().to_frame(index=False)
metadata

# %%
set(metadata["scenario"])

# %%
metadata[["climate_model", "model", "scenario"]].drop_duplicates()

# %%
set(metadata["variable"])

# %%
len(set(metadata["run_id"]))

# %% [markdown]
# ## Load a few variables

# %%
variables = [
    "Surface Air Temperature Change",
    "Atmospheric Concentrations|CO2",
    "Atmospheric Concentrations|CH4",
]
data = db.load(pix.isin(variable=variables))
data.head()

# %% [markdown]
# ## Temperature plume (all scenarios)

# %%
temperature = data.loc[pix.isin(variable="Surface Air Temperature Change"), 2000:]
temperature_median = temperature.openscm.groupby_except("run_id").median()

fig, ax = plt.subplots()
for idx, row in temperature_median.iterrows():
    ax.plot(row.index, row.values, label=idx[temperature_median.index.names.index("scenario")])
ax.set_ylabel("K")
ax.set_title("Surface Air Temperature Change (median over run_id)")
ax.legend(fontsize="small")

# %% [markdown]
# ## CO2 and CH4 concentrations: median and 5-95% range

# %%
fig, axes = plt.subplots(1, 2, figsize=(11, 4))
for ax, variable in zip(axes, ["Atmospheric Concentrations|CO2", "Atmospheric Concentrations|CH4"]):
    subset = data.loc[pix.isin(variable=variable), 2000:]
    median = subset.openscm.groupby_except("run_id").median()
    lower = subset.openscm.groupby_except("run_id").quantile(0.05)
    upper = subset.openscm.groupby_except("run_id").quantile(0.95)
    for i in range(median.shape[0]):
        ax.plot(median.columns, median.iloc[i].values)
        ax.fill_between(median.columns, lower.iloc[i].values, upper.iloc[i].values, alpha=0.2)
    ax.set_title(variable)
    ax.set_ylabel(median.pix.unique("unit")[0])

# %% [markdown]
# ## CO2 and CH4 concentrations: mean and ±1 standard deviation over run_id

# %%
fig, axes = plt.subplots(1, 2, figsize=(11, 4))
for ax, variable in zip(axes, ["Atmospheric Concentrations|CO2", "Atmospheric Concentrations|CH4"]):
    subset = data.loc[pix.isin(variable=variable), 2000:]
    mean = subset.openscm.groupby_except("run_id").mean()
    std = subset.openscm.groupby_except("run_id").std()
    for i in range(mean.shape[0]):
        ax.plot(mean.columns, mean.iloc[i].values)
        ax.fill_between(mean.columns, mean.iloc[i].values - std.iloc[i].values, mean.iloc[i].values + std.iloc[i].values, alpha=0.2)
    ax.set_title(variable)
    ax.set_ylabel(mean.pix.unique("unit")[0])

# %%
