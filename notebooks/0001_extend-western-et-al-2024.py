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
import openscm_units
import pandas as pd
import pandas_indexing as pix
import pandas_openscm
import pint
import seaborn as sns

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Parameters

# %% editable=true slideshow={"slide_type": ""} tags=["parameters"]
ghg: str = "hcfc22"
western_et_al_2024_clean_path: str = (
    "../output-bundles/dev-test/data/interim/western-et-al-2024/cleaned-mixing-ratios.feather"
)
wmo_2022_clean_path: str = "../output-bundles/dev-test/data/interim/wmo-2022/cleaned-mixing-ratios.feather"
out_file: str = "../output-bundles/dev-test/data/interim/western-et-al-2024/extended-mixing-ratios.feather"


# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Parse parameters

# %% editable=true slideshow={"slide_type": ""}
western_et_al_2024_clean_p = Path(western_et_al_2024_clean_path)
wmo_2022_clean_p = Path(wmo_2022_clean_path)
out_file_p = Path(out_file)

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Set up

# %% editable=true slideshow={"slide_type": ""}
pandas_openscm.register_pandas_accessor()
pint.set_application_registry(openscm_units.unit_registry)
Q = openscm_units.unit_registry.Quantity

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Load data

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ### Cleaned data

# %%
western_et_al_2024_clean = pd.read_feather(western_et_al_2024_clean_p)
western_et_al_2024_ghg = western_et_al_2024_clean.loc[pix.isin(ghg=ghg)]
# western_et_al_2024_ghg

# %% editable=true slideshow={"slide_type": ""}
wmo_2022_clean = pd.read_feather(wmo_2022_clean_path)
wmo_2022_ghg = wmo_2022_clean.loc[pix.isin(ghg=ghg)]
# wmo_2022_ghg

# %% [markdown]
# ## Compare raw data

# %%
pdf = pix.concat(
    [
        wmo_2022_ghg.pix.assign(source="WMO 2022"),
        western_et_al_2024_ghg.pix.assign(source="Western et al. 2024"),
    ]
)

fg = sns.relplot(
    data=pdf.openscm.to_long_data(), x="time", y="value", col="ghg", hue="source", facet_kws=dict(sharey=False)
)

# %% [markdown]
# ## Extend
#
# We extend the Western et al. data using some very basic assumptions.

# %% [markdown]
# We assume that the gas' atmospheric concentration can be modelled by a simple one-box model
#
# $$
# \frac{dC}{dt} = \alpha E - \frac{C}{\tau}
# $$
#
# We solve for emissions in 2100 using the gradient and concentration
# in the last year.

# %%
last_year = western_et_al_2024_ghg.columns.max()
last_year

# %%
unit_l = western_et_al_2024_ghg.pix.unique("unit")
if len(unit_l) != 1:
    raise AssertionError(unit_l)

unit = unit_l[0]
unit

# %%
taus = {
    "hcfc22": Q(11.6, "yr"),
    "hcfc141b": Q(8.81, "yr"),
    "hcfc142b": Q(17.1, "yr"),
}
tau = taus[ghg]
tau

# %%
# Check annual timesteps
np.testing.assert_equal(
    western_et_al_2024_ghg.columns,
    np.arange(western_et_al_2024_ghg.columns.min(), western_et_al_2024_ghg.columns.max() + 1),
)

timestep = Q(1, "yr")

# %%
alpha_e_last_year = (
    Q(western_et_al_2024_ghg.loc[(unit, ghg), last_year], unit)
    - Q(western_et_al_2024_ghg.loc[(unit, ghg), last_year - 1], unit)
) / timestep + Q(western_et_al_2024_ghg.loc[(unit, ghg), last_year], unit) / tau
alpha_e_last_year

# %% [markdown]
# Then, we assume that from 2100 on, emissions are constant.
# This is a conservative assumption,
# hence we feel ok about it.
# With this, we can run a very basic forward model to extend.

# %%
western_et_al_2024_ghg_extended = western_et_al_2024_ghg.copy()
missing_years = np.setdiff1d(wmo_2022_ghg.columns, western_et_al_2024_ghg_extended.columns)
western_et_al_2024_ghg_extended.loc[:, missing_years] = np.nan

for y in sorted(missing_years):
    dC_dt = alpha_e_last_year - Q(western_et_al_2024_ghg_extended.loc[(unit, ghg), y - 1], unit) / tau

    western_et_al_2024_ghg_extended[y] = western_et_al_2024_ghg_extended[y - 1] + (timestep * dC_dt).to(unit).m

western_et_al_2024_ghg_extended

# %%
pdf = pix.concat(
    [
        wmo_2022_ghg.pix.assign(source="WMO 2022"),
        western_et_al_2024_ghg.pix.assign(source="Western et al. 2024"),
        western_et_al_2024_ghg_extended.pix.assign(source="Western et al. 2024 extended"),
    ]
)

fig, axes = plt.subplots(ncols=2, figsize=(10, 4))
ax = sns.lineplot(data=pdf.openscm.to_long_data(), x="time", y="value", hue="source", alpha=0.3, ax=axes[0])
ax.axhline(0.0, linestyle="--", color="gray")

ax = sns.lineplot(
    data=pdf.loc[:, last_year - 10 : last_year + 10].openscm.to_long_data(),
    x="time",
    y="value",
    hue="source",
    alpha=0.3,
    ax=axes[1],
)

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Save

# %% editable=true slideshow={"slide_type": ""}
out_file_p.parent.mkdir(parents=True, exist_ok=True)
western_et_al_2024_ghg_extended.to_feather(out_file_p)
