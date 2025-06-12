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
# # Plot marker overview

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
import pandas_openscm.db
import pint
import seaborn as sns
import tqdm.auto
import xarray as xr

from cmip7_scenariomip_ghg_generation.constants import GHG_LIFETIMES, GHG_MOLECULAR_MASSES, Q
from cmip7_scenariomip_ghg_generation.scenario_info import ScenarioInfo

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Parameters

# %% editable=true slideshow={"slide_type": ""} tags=["parameters"]
ghg: str = "hfc23"
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

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Set up

# %% editable=true slideshow={"slide_type": ""}
pint.set_application_registry(openscm_units.unit_registry)
pandas_openscm.register_pandas_accessor()

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Load data

# %%
emissions = pix.concat(
    [pd.read_feather(emissions_complete_dir_p / f"{si.to_file_stem()}.feather") for si in scenario_info_markers_p]
)

# emissions

# %%
magiccc_output_l = []
for si in tqdm.auto.tqdm(scenario_info_markers_p):
    magiccc_output_l.append(
        magicc_output_db.load(
            pix.isin(
                model=si.model,
                scenario=si.scenario,
                climate_model="MAGICCv7.6.0a3",
            )
            & pix.ismatch(variable=["Atmospheric Concentrations**"]),
            # progress=True,
        )
    )

magiccc_output = pix.concat(magiccc_output_l)
# magiccc_output

# %%
historical_concs_l = []
for hist_gm_path in tqdm.auto.tqdm(historical_data_root_dir_p.rglob("**/yr/**/*gm*.nc")):
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
one_box_projections_l = []
for ghg, lifetime in GHG_LIFETIMES.items():
    # if ghg not in ["ccl4", "hcfc141b"]:
    #     continue

    molecular_mass = GHG_MOLECULAR_MASSES[ghg]

    alpha = 1 / (atm_moles * fraction_factor * molecular_mass)
    ghg_units = (1 / (alpha * fraction_factor * Q(1, "kg"))).to_base_units().units
    out_unit = "ppt"
    alpha = alpha.to(f"{out_unit} / t{ghg_units}")
    alpha_m = alpha.m
    # alpha

    historical_concs_ghg = historical_concs.loc[pix.isin(ghg=ghg)]
    if historical_concs_ghg.empty:
        print(f"Need to get historical {ghg}")
        continue
        # raise AssertionError

    emissions = emissions_pdf.loc[emissions_pdf.index.get_level_values("variable").str.lower().str.endswith(ghg)]
    emissions_m = emissions.pix.convert_unit(f"t{ghg_units}/yr")

    years = np.arange(historical_concs_ghg.columns.max(), emissions_m.columns.max())
    one_box_projection = pd.DataFrame(
        np.zeros((emissions_m.shape[0], years.size)), columns=years, index=emissions_m.index
    ).pix.assign(variable=f"Atmospheric Concentrations|{ghg}", unit=out_unit)
    one_box_projection[historical_concs_ghg.columns.max()] = historical_concs_ghg[
        historical_concs_ghg.columns.max()
    ].values.squeeze()

    for yr in one_box_projection.columns[1:]:
        dC_dt = (alpha_m * emissions_m[yr - 1]).values - one_box_projection[yr - 1] / lifetime.to("yr").m
        one_box_projection[yr] = (
            one_box_projection[yr - 1] + 1 * dC_dt  # implicit one-year timestep
        )
        # break

    one_box_projections_l.append(one_box_projection)

one_box_projections = pix.concat(one_box_projections_l)
# one_box_projections

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
for variable, one_box_projection in tqdm.auto.tqdm(one_box_projections.groupby("variable")):
    ghg = variable.split("Atmospheric Concentrations|")[1]
    emissions = emissions_pdf.loc[emissions_pdf.index.get_level_values("variable").str.lower().str.endswith(ghg)]

    fig, axes = plt.subplots(nrows=3, figsize=(6, 10))

    sns.lineplot(
        data=emissions.openscm.to_long_data(),
        x="time",
        y="value",
        hue="cmip_scenario_name",
        style="variable",
        ax=axes[0],
    )
    sns.move_legend(axes[0], loc="center left", bbox_to_anchor=(1.05, 0.5))

    historical_concs_tmp = historical_concs.loc[pix.isin(ghg=ghg)].pix.assign(
        source="historical-ghgs", cmip_scenario_name="historical"
    )
    if historical_concs_tmp.empty:
        print(f"Need historical concs for {ghg}")
        continue

    magiccc_output_pdf_tmp = (
        magiccc_output_pdf.loc[magiccc_output_pdf.index.get_level_values("variable").str.lower().str.endswith(ghg)]
        .openscm.groupby_except("run_id")
        .median()
        .pix.assign(source="MAGICC")
    )

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

    for ax, xlim in ((axes[1], (1750, 2100)), (axes[2], (2000, 2050))):
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

    plt.suptitle(ghg)
    plt.show()

    # break

# %%
