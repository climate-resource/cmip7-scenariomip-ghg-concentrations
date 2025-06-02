# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.17.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown] editable=true slideshow={"slide_type": ""}
# # Extend Western et al. (2024) projections with WMO (2022) projections

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Imports

# %% editable=true slideshow={"slide_type": ""}
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pandas_indexing as pix
import pandas_openscm
import seaborn as sns

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Parameters

# %% editable=true slideshow={"slide_type": ""} tags=["parameters"]
western_et_al_2024_clean_path: str = (
    "../output-bundles/dev-test/data/interim/western-et-al-2024/cleaned-mixing-ratios.feather"
)
wmo_2022_clean_path: str = "../output-bundles/dev-test/data/interim/wmo-2022/cleaned-mixing-ratios.feather"
western_et_al_2024_extended_path: str = (
    "../output-bundles/dev-test/data/interim/western-et-al-2024/extended-mixing-ratios.feather"
)


# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Parse parameters

# %% editable=true slideshow={"slide_type": ""}
western_et_al_2024_clean_p = Path(western_et_al_2024_clean_path)
wmo_2022_clean_p = Path(wmo_2022_clean_path)
western_et_al_2024_extended_p = Path(western_et_al_2024_extended_path)

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Set up

# %% editable=true slideshow={"slide_type": ""}
pandas_openscm.register_pandas_accessor()

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Load data

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ### Cleaned data

# %%
western_et_al_2024_clean = pd.read_feather(western_et_al_2024_clean_p)
# western_et_al_2024_clean

# %% editable=true slideshow={"slide_type": ""}
wmo_2022_clean = pd.read_feather(wmo_2022_clean_path)
# wmo_2022_clean

# %%
wmo_2022_clean_of_interest = wmo_2022_clean.loc[pix.isin(ghg=western_et_al_2024_clean.pix.unique("ghg"))]
# wmo_2022_clean_of_interest

# %% [markdown]
# ## Compare raw data

# %%
pdf = pix.concat(
    [
        wmo_2022_clean_of_interest.pix.assign(source="WMO 2022"),
        western_et_al_2024_clean.pix.assign(source="Western et al. 2024"),
    ]
)

sns.relplot(data=pdf.openscm.to_long_data(), x="time", y="value", col="ghg", hue="source", facet_kws=dict(sharey=False))

# %%
assert False
# Do some basic extension using an exponential roll off
# with hard-coded decay times and floors based on expert judgement.
# Re-invert all concentrations (including WMO 2022 based stuff)
# before doing latitudinal gradient emission extensions
# Push new inversions back to emissions harmonisation historical repo

# %% [markdown]
# ## Extend

# %%
western_et_al_2024_clean

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Adjust raw data to middle of year
#
# Assumption: raw data is for 1 Jan, we need middle of year.
# Fine as we check alignment of historical and source later.

# %% editable=true slideshow={"slide_type": ""}
source_mid_year = (raw_data_ghg + raw_data_ghg.shift(-1, axis="columns")) / 2.0
# Super basic extrapolation, will be fine as it is for 2500
source_mid_year[2500] = 2 * source_mid_year[2499] - source_mid_year[2498]
# ax = wmo_mid_year.loc[:, 2010: 2020].T.plot()
ax = source_mid_year.loc[:, :].T.plot()
ax.grid()
plt.show()

source_mid_year

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Harmonisation

# %% editable=true slideshow={"slide_type": ""}
last_history_year = int(cmip7_historical_gm["time"].dt.year.max())
last_history_year

# %%
fig, ax = plt.subplots()

cmm = cmip7_historical_gm.groupby("time.year").mean()
ax.plot(
    cmm["year"],
    cmm.values,
    label="CMIP7 historical",
)
ax.plot(source_mid_year.columns, source_mid_year.values.squeeze(), label="WMO 2022", linewidth=2, alpha=0.5)

ax.set_xlim([2000, 2050])
ax.grid()
ax.legend()
ax.set_title(ghg)

# %%
np.testing.assert_allclose(
    cmip7_historical_gm.sel(time=cmip7_historical_gm["time"].dt.year == last_history_year),
    source_mid_year.loc[:, last_history_year],
    rtol=2e-3,
)

harmonised = source_mid_year

# %% editable=true slideshow={"slide_type": ""}
fig, ax = plt.subplots()


source_mid_year.pix.assign(label="WMO 2022").pix.project(["label", "ghg", "unit"]).T.plot(ax=ax, linewidth=2, alpha=0.5)
harmonised.pix.assign(label="harmonised").pix.project(["label", "ghg", "unit"]).T.plot(ax=ax, linewidth=2, alpha=0.5)
ax.plot(
    cmm["year"],
    cmm.values,
    label="CMIP7 historical",
)

ax.set_xlim([2000, 2100])
ax.grid()
ax.legend()

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Save

# %% editable=true slideshow={"slide_type": ""}
out_file_p.parent.mkdir(parents=True, exist_ok=True)
harmonised.loc[:, last_history_year:].to_feather(out_file_p)
