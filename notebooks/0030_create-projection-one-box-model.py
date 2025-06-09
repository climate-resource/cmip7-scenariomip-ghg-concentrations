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

# %% [markdown] editable=true slideshow={"slide_type": ""}
# # Create annual-means for gases that can projected with a one-box model

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Imports

# %% editable=true slideshow={"slide_type": ""}
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pandas_indexing as pix
import pandas_openscm
import pandas_openscm.db
import seaborn as sns
import tqdm.auto
import xarray as xr

from cmip7_scenariomip_ghg_generation.constants import (
    GHG_LIFETIMES,
    GHG_MOLECULAR_MASSES,
    GHG_RADIATIVE_EFFICIENCIES,
    Q,
)
from cmip7_scenariomip_ghg_generation.scenario_info import ScenarioInfo

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Parameters

# %% editable=true slideshow={"slide_type": ""} tags=["parameters"]
ghg: str = "sf6"
scenario_info_markers: str = (
    "WITCH 6.0;SSP5 - Medium-Low Emissions_a;hl;;"
    "REMIND-MAgPIE 3.5-4.10;SSP1 - Very Low Emissions;vllo;;"
    "MESSAGEix-GLOBIOM-GAINS 2.1-M-R12;SSP2 - Low Emissions;l;;"
    "IMAGE 3.4;SSP2 - Medium Emissions;m;;"
    "GCAM 7.1 scenarioMIP;SSP3 - High Emissions;h;;"
    "AIM 3.0;SSP2 - Low Overshoot;vlho;;"
    "COFFEE 1.6;SSP2 - Medium-Low Emissions;ml"
)
emissions_complete_dir: str = "../output-bundles/dev-test/data/interim/complete-emissions"
historical_data_root_dir: str = "../output-bundles/dev-test/data/raw/historical-ghg-concs"
magicc_output_db_dir: str = "../output-bundles/dev-test/data/interim/magicc-output/db"
magicc_db_backend_str: str = "feather"
out_file: str = "../output-bundles/dev-test/data/interim/annual-means/one-box_sf6_annual-mean.feather"


# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Parse parameters

# %% editable=true slideshow={"slide_type": ""}
siml = []
for v in scenario_info_markers.split(";;"):
    model, scenario, cmip_scenario_name = v.split(";")
    siml.append(
        ScenarioInfo(
            cmip_scenario_name=cmip_scenario_name,
            model=model,
            scenario=scenario,
        )
    )

scenario_info_markers_p = tuple(siml)
emissions_complete_dir_p = Path(emissions_complete_dir)
historical_data_root_dir_p = Path(historical_data_root_dir)
magicc_output_db = pandas_openscm.db.OpenSCMDB(
    backend_data=pandas_openscm.db.DATA_BACKENDS.get_instance(magicc_db_backend_str),
    backend_index=pandas_openscm.db.INDEX_BACKENDS.get_instance(magicc_db_backend_str),
    db_dir=Path(magicc_output_db_dir),
)
out_file_p = Path(out_file)

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Set up

# %% editable=true slideshow={"slide_type": ""}
pandas_openscm.register_pandas_accessor()

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Load data

# %%
emissions = pix.concat(
    [pd.read_feather(emissions_complete_dir_p / f"{si.to_file_stem()}.feather") for si in scenario_info_markers_p]
)

# emissions

# %%
magicc_output_db_reader = magicc_output_db.create_reader()
# magicc_output_db_reader

# %%
variable_magicc_l = [
    v
    for v in magicc_output_db_reader.metadata.get_level_values("variable").unique()
    if v.lower().endswith(ghg) and v.startswith("Atmospheric Concentrations")
]
if len(variable_magicc_l) != 1:
    raise AssertionError(variable_magicc_l)

variable_magicc = variable_magicc_l[0]
variable_magicc

# %%
magiccc_output_l = []
for si in tqdm.auto.tqdm(scenario_info_markers_p):
    magiccc_output_l.append(
        magicc_output_db_reader.load(
            pix.isin(
                model=si.model,
                scenario=si.scenario,
                climate_model="MAGICCv7.6.0a3",
            )
            & pix.isin(variable=variable_magicc),
            # progress=True,
        )
    )

magiccc_output = pix.concat(magiccc_output_l)
# magiccc_output

# %%
historical_concs_l = []
for hist_gm_path in tqdm.auto.tqdm(historical_data_root_dir_p.rglob(f"**/yr/**/{ghg}/**/*gm*.nc")):
    ghg_var = hist_gm_path.name.split("_")[0]
    da = xr.load_dataset(hist_gm_path)[ghg_var]
    df = (
        da.groupby("time.year")
        .mean()
        .to_dataframe()
        .T.rename_axis("ghg")
        .pix.assign(unit=da.attrs["units"], scenario="historical")
    )

    historical_concs_l.append(df)

historical_concs = pix.concat(historical_concs_l)
# historical_concs

# %% [markdown]
# ## Add useful metadata to data

# %%
cmip_scenario_name_d = {(si.model, si.scenario): si.cmip_scenario_name for si in scenario_info_markers_p}
cmip_scenario_name_d


# %%
def add_cmip_scenario_name(indf: pd.DataFrame) -> pd.DataFrame:
    """Add CMIP scenario name to the index"""
    cmip_scenario_names = indf.index.droplevel(indf.index.names.difference(["model", "scenario"])).map(
        cmip_scenario_name_d
    )
    res = indf.openscm.set_index_levels({"cmip_scenario_name": cmip_scenario_names})

    return res


# %%
emissions_pdf = add_cmip_scenario_name(emissions)
# emissions_pdf

# %%
magiccc_output_pdf = add_cmip_scenario_name(magiccc_output)
# magiccc_output_pdf

# %% [markdown]
# ## Calculate one-box equivalents

# %%
# CDIAC https://web.archive.org/web/20170118004650/http://cdiac.ornl.gov/pns/convert.html
ATMOSPHERE_MASS = Q(5.137 * 10**18, "kg")
# https://www.engineeringtoolbox.com/molecular-mass-air-d_679.html
MOLAR_MASS_DRY_AIR = Q(28.9, "g / mol")
atm_moles = (ATMOSPHERE_MASS / MOLAR_MASS_DRY_AIR).to("mole")
# Lines up with CDIAC: https://web.archive.org/web/20170118004650/http://cdiac.ornl.gov/pns/convert.html
fraction_factor = Q(1e-6, "1 / ppm")
mass_one_ppm_co2 = atm_moles * fraction_factor * Q(12.01, "gC / mole")
cdiac_expected = 2.13
if np.round(mass_one_ppm_co2.to("GtC / ppm").m, 2) != cdiac_expected:
    raise AssertionError

# %%
lifetime = GHG_LIFETIMES[ghg]
lifetime

# %%
molecular_mass = GHG_MOLECULAR_MASSES[ghg]
molecular_mass

# %%
alpha = 1 / (atm_moles * fraction_factor * molecular_mass)
alpha

# %%
historical_concs_ghg = historical_concs.loc[pix.isin(ghg=ghg)]
if historical_concs_ghg.empty:
    raise AssertionError

out_unit_l = historical_concs_ghg.pix.unique("unit")
if len(out_unit_l) != 1:
    raise AssertionError(out_unit_l)

out_unit = out_unit_l[0]
# out_unit

# %%
emissions_ghg = emissions_pdf.loc[emissions_pdf.index.get_level_values("variable").str.lower().str.endswith(ghg)]
emissions_ghg_unit_l = emissions_ghg.pix.unique("unit")
if len(emissions_ghg_unit_l) != 1:
    raise AssertionError(emissions_ghg_unit_l)

emissions_ghg_unit = emissions_ghg_unit_l[0]
# emissions_ghg

# %%
years = np.arange(historical_concs_ghg.columns.max(), emissions_ghg.columns.max() + 1)
one_box_projection_arr = Q(np.zeros((emissions_ghg.shape[0], years.size)), out_unit)
one_box_projection_arr[:, 0] = Q(historical_concs_ghg[historical_concs_ghg.columns.max()].values.squeeze(), out_unit)

for i, yr in enumerate(years[1:]):
    # This isn't perfect with mid-year vs. start-year probably, but ok,
    # we can't solve that without continuous functions anyway.
    idx = i + 1
    C = one_box_projection_arr[:, idx - 1]
    emissions = Q(emissions_ghg[yr - 1].values, emissions_ghg_unit)
    for _ in range(12):
        dt = Q(1 / 12.0, "yr")
        dC_dt = (alpha * emissions) - C / lifetime
        C = C + dt * dC_dt

    one_box_projection_arr[:, idx] = C

# one_box_projection_arr

# %%
one_box_projection = pd.DataFrame(
    one_box_projection_arr.to("ppt").m,
    columns=years,
    index=emissions_ghg.index,
).pix.assign(variable=f"Atmospheric Concentrations|{ghg}", unit="ppt")

one_box_projection

# %% [markdown]
# ## Plot

# %%
palette = {
    "vllo": "#24a4ff",
    "vlho": "#4a0daf",
    "l": "#00cc69",
    "ml": "#f5ac00",
    "m": "#ffa9dc",
    "h": "#700000",
    "hl": "#8f003b",
}

scenario_order = ["vllo", "vlho", "l", "ml", "m", "hl", "h"]

# %%
fig, axes = plt.subplots(ncols=2, nrows=2, figsize=(12, 8))

sns.lineplot(
    data=emissions_ghg.openscm.to_long_data(),
    x="time",
    y="value",
    hue="cmip_scenario_name",
    style="variable",
    ax=axes[0][0],
)
axes[0][0].set_title("Concentrations")

historical_concs_tmp = historical_concs_ghg.pix.assign(source="historical-ghgs", cmip_scenario_name="historical")

magiccc_output_pdf_tmp = magiccc_output_pdf.openscm.groupby_except("run_id").median().pix.assign(source="MAGICC")

pdf_conc = pix.concat(
    [
        one_box_projection.pix.assign(source="one-box").reset_index(
            one_box_projection.index.names.difference(["cmip_scenario_name", "source", "unit"]), drop=True
        ),
        historical_concs_tmp.reset_index(
            historical_concs_tmp.index.names.difference(["cmip_scenario_name", "source", "unit"]), drop=True
        ),
        magiccc_output_pdf_tmp.reset_index(
            magiccc_output_pdf_tmp.index.names.difference(["cmip_scenario_name", "source", "unit"]), drop=True
        ),
    ]
).sort_index(axis="columns")

for ax, xlim in ((axes[1][0], (1750, 2100)), (axes[1][1], (2000, 2050))):
    sns.lineplot(
        data=pdf_conc.loc[:, xlim[0] : xlim[1]].openscm.to_long_data(),
        x="time",
        y="value",
        hue="cmip_scenario_name",
        style="source",
        ax=ax,
    )
    ax.set_xlim(xlim)
    sns.move_legend(ax, loc="center left", bbox_to_anchor=(1.05, 0.5))

axes[1][0].legend().remove()

pdf_erf = (GHG_RADIATIVE_EFFICIENCIES[ghg].to("W / m^2 / ppt").m * pdf_conc).pix.assign(unit="W / m^2")
sns.lineplot(
    data=pdf_erf.openscm.to_long_data(),
    x="time",
    y="value",
    hue="cmip_scenario_name",
    style="source",
    ax=axes[0][1],
)
ax.set_xlim(xlim)
sns.move_legend(axes[0][1], loc="center left", bbox_to_anchor=(1.05, 0.5))
axes[0][1].set_ylim((0.0, 0.25))
axes[0][1].set_title("ERF")

plt.suptitle(ghg)
plt.show()

# %%
one_box_projection[2022]

# %%
historical_concs_ghg[2022]

# %% [markdown]
# ## Save

# %%
out = one_box_projection.pix.assign(
    ghg=ghg, scenario=one_box_projection.index.get_level_values("cmip_scenario_name")
).pix.project(["unit", "scenario", "ghg"])
# out

# %%
out_file_p.parent.mkdir(exist_ok=True, parents=True)
out.to_feather(out_file_p)
