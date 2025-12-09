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
# # Run MAGICC concentration-driven
#
# This shows the temperature response to the concentrations that are actually produced,
# which aren't idential to MAGICC's output as a result of
# different history, harmonisation
# and the use of e.g. WMO scenarios directly.

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Imports

# %% editable=true slideshow={"slide_type": ""}
import json
import os
import platform
from functools import partial
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pandas_indexing as pix
import pandas_openscm
import pandas_openscm.db
import pint
import seaborn as sns
import tqdm.auto
import xarray as xr
from gcages.renaming import SupportedNamingConventions, convert_variable_name
from gcages.scm_running import convert_openscm_runner_output_names_to_magicc_output_names, run_scms
from IPython import display
from pymagicc.definitions import convert_magicc7_to_openscm_variables
from pymagicc.io import MAGICCData

from cmip7_scenariomip_ghg_generation.scenario_info import ScenarioInfo

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Parameters

# %% editable=true slideshow={"slide_type": ""} tags=["parameters"]
cmip_scenario_name: str = "vl"
model: str = "REMIND-MAgPIE 3.5-4.11"
scenario: str = "SSP1 - Very Low Emissions"
emissions_complete_dir: str = "../output-bundles/dev-test/data/interim/complete-emissions"
magicc_output_db_dir: str = "../output-bundles/dev-test/data/interim/magicc-output/db"
magicc_db_backend_str: str = "feather"
esgf_ready_root_dir: str = "../output-bundles/dev-test/data/processed/esgf-ready"
historical_data_root_dir: str = "../output-bundles/dev-test/data/raw/historical-ghg-concs"
magicc_version: str = "MAGICCv7.6.0a3"
magicc_exe: str = "../magicc/magicc-v7.6.0a3/bin/magicc-darwin-arm64"
magicc_prob_distribution: str = "../magicc/magicc-v7.6.0a3/configs/magicc-ar7-fast-track-drawnset-v0-3-0.json"
n_magicc_workers: int = 4


# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Parse parameters

# %% editable=true slideshow={"slide_type": ""}
scenario_info = ScenarioInfo(model=model, scenario=scenario, cmip_scenario_name=cmip_scenario_name)
emissions_complete_dir_p = Path(emissions_complete_dir)
magicc_output_db = pandas_openscm.db.OpenSCMDB(
    backend_data=pandas_openscm.db.DATA_BACKENDS.get_instance(magicc_db_backend_str),
    backend_index=pandas_openscm.db.INDEX_BACKENDS.get_instance(magicc_db_backend_str),
    db_dir=Path(magicc_output_db_dir),
)
esgf_ready_root_dir_p = Path(esgf_ready_root_dir)
historical_data_root_dir_p = Path(historical_data_root_dir)
magicc_exe_p = Path(magicc_exe)
magicc_prob_distribution_p = Path(magicc_prob_distribution)

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Set up

# %% editable=true slideshow={"slide_type": ""}
pix.set_openscm_registry_as_default()
UR = pint.get_application_registry()
pandas_openscm.register_pandas_accessor()

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Load data

# %% [markdown]
# ### Emissions

# %%
emissions = pd.read_feather(emissions_complete_dir_p / f"{scenario_info.to_file_stem()}.feather")

# emissions

# %% [markdown]
# ### Concentrations

# %% [markdown]
# #### Historical

# %%
historical_concentrations_xr_l = []
for fp in tqdm.auto.tqdm(historical_data_root_dir_p.rglob("**/yr/**/*gm*.nc")):
    ghg = fp.name.split("_")[0]
    historical_concentrations_xr_l.append(xr.open_dataset(fp)[ghg])

historical_concentrations_xr = xr.merge(historical_concentrations_xr_l)
historical_concentrations_xr

# %% [markdown]
# #### Future

# %%
concentrations_xr_l = []
source_id = None
for fp in tqdm.auto.tqdm(esgf_ready_root_dir_p.rglob(f"**/yr/**/*{cmip_scenario_name}*gm*.nc")):
    source_id_fp = fp.name.split("_")[4]
    if source_id is None:
        source_id = source_id_fp
    elif source_id != source_id_fp:
        raise AssertionError(source_id_fp)

    ghg = fp.name.split("_")[0]
    if ghg.endswith("eq"):
        # Don't need equivalent species for MAGICC
        continue

    concentrations_xr_l.append(xr.open_dataset(fp)[ghg])

concentrations_xr = xr.merge(concentrations_xr_l)
# concentrations_xr

# %% [markdown]
# ## Write concentrations files for MAGICC and set config

# %%
CONC_MAGICC_FLAG_MAP = {
    "Atmospheric Concentrations|CO2": "file_co2_conc",
    "Atmospheric Concentrations|CH4": "file_ch4_conc",
    "Atmospheric Concentrations|N2O": "file_n2o_conc",
    "Atmospheric Concentrations|CF4": "fgas_files_conc__0",
    "Atmospheric Concentrations|C2F6": "fgas_files_conc__1",
    "Atmospheric Concentrations|C3F8": "fgas_files_conc__2",
    "Atmospheric Concentrations|C4F10": "fgas_files_conc__3",
    "Atmospheric Concentrations|C5F12": "fgas_files_conc__4",
    "Atmospheric Concentrations|C6F14": "fgas_files_conc__5",
    "Atmospheric Concentrations|C7F16": "fgas_files_conc__6",
    "Atmospheric Concentrations|C8F18": "fgas_files_conc__7",
    "Atmospheric Concentrations|cC4F8": "fgas_files_conc__8",
    "Atmospheric Concentrations|HFC23": "fgas_files_conc__9",
    "Atmospheric Concentrations|HFC32": "fgas_files_conc__10",
    "Atmospheric Concentrations|HFC4310": "fgas_files_conc__11",
    "Atmospheric Concentrations|HFC125": "fgas_files_conc__12",
    "Atmospheric Concentrations|HFC134a": "fgas_files_conc__13",
    "Atmospheric Concentrations|HFC143a": "fgas_files_conc__14",
    "Atmospheric Concentrations|HFC152a": "fgas_files_conc__15",
    "Atmospheric Concentrations|HFC227ea": "fgas_files_conc__16",
    "Atmospheric Concentrations|HFC236fa": "fgas_files_conc__17",
    "Atmospheric Concentrations|HFC245fa": "fgas_files_conc__18",
    "Atmospheric Concentrations|HFC365mfc": "fgas_files_conc__19",
    "Atmospheric Concentrations|NF3": "fgas_files_conc__20",
    "Atmospheric Concentrations|SF6": "fgas_files_conc__21",
    "Atmospheric Concentrations|SO2F2": "fgas_files_conc__22",
    "Atmospheric Concentrations|CFC11": "mhalo_files_conc__0",
    "Atmospheric Concentrations|CFC12": "mhalo_files_conc__1",
    "Atmospheric Concentrations|CFC113": "mhalo_files_conc__2",
    "Atmospheric Concentrations|CFC114": "mhalo_files_conc__3",
    "Atmospheric Concentrations|CFC115": "mhalo_files_conc__4",
    "Atmospheric Concentrations|HCFC22": "mhalo_files_conc__5",
    "Atmospheric Concentrations|HCFC141b": "mhalo_files_conc__6",
    "Atmospheric Concentrations|HCFC142b": "mhalo_files_conc__7",
    "Atmospheric Concentrations|CH3CCl3": "mhalo_files_conc__8",
    "Atmospheric Concentrations|CCl4": "mhalo_files_conc__9",
    "Atmospheric Concentrations|CH3Cl": "mhalo_files_conc__10",
    "Atmospheric Concentrations|CH2Cl2": "mhalo_files_conc__11",
    "Atmospheric Concentrations|CHCl3": "mhalo_files_conc__12",
    "Atmospheric Concentrations|CH3Br": "mhalo_files_conc__13",
    "Atmospheric Concentrations|Halon1211": "mhalo_files_conc__14",
    "Atmospheric Concentrations|Halon1301": "mhalo_files_conc__15",
    "Atmospheric Concentrations|Halon2402": "mhalo_files_conc__16",
    "Atmospheric Concentrations|Halon1202": "mhalo_files_conc__17",
}


# %%
def to_df(da: xr.DataArray, variable: str) -> pd.DataFrame:
    """
    Convert to pandas DataFrame
    """
    return (
        da.groupby("time.year")
        .mean()
        .to_dataframe()
        .T.rename_axis("variable")
        .pix.assign(variable=variable, scenario=scenario, model=model, unit=da.attrs["units"], region="World")
    )


# %%
magicc_run_dir = magicc_exe_p.parents[1] / "run"
magicc_file_dir = magicc_run_dir / "cmip7-ghgs" / source_id
magicc_file_dir.mkdir(exist_ok=True, parents=True)

# %%
magicc_concentration_cfg = {
    "file_co2_conc": None,
    "co2_switchfromconc2emis_year": 10000,
    "file_ch4_conc": None,
    "ch4_switchfromconc2emis_year": 10000,
    "file_n2o_conc": None,
    "n2o_switchfromconc2emis_year": 10000,
    "fgas_files_conc": [None] * 23,
    "fgas_switchfromconc2emis_year": 10000,
    "mhalo_files_conc": [None] * 18,
    "mhalo_switchfromconc2emis_year": 10000,
}
# Halon1202, just ignore essentially
magicc_concentration_cfg["mhalo_files_conc"][17] = ""

# %%
for ghg in tqdm.auto.tqdm(concentrations_xr.data_vars):
    ghg_magicc = ghg.replace("hfc4310mee", "hfc4310")

    openscm_runner_variable = convert_magicc7_to_openscm_variables(f"{ghg_magicc}_conc".upper())

    complete_timeseries = pix.concat(
        [
            to_df(historical_concentrations_xr[ghg], variable=openscm_runner_variable),
            to_df(concentrations_xr[ghg], variable=openscm_runner_variable),
        ],
        axis="columns",
    )

    # Check data is annual, but can skip here as we already know it's annual
    exp_years = np.arange(complete_timeseries.columns.min(), complete_timeseries.columns.max() + 1)
    np.testing.assert_equal(
        complete_timeseries.columns.values,
        exp_years,
    )

    writer = MAGICCData(complete_timeseries.copy())
    writer["todo"] = "SET"
    writer.metadata = {
        "header": f"tmp {cmip_scenario_name}",
        # TODO: better provenance
        "source": f"CMIP7 ScenarioMIP GHGs {source_id}",
    }

    fn = magicc_file_dir / f"{cmip_scenario_name}_{ghg_magicc}_CONC.IN".upper()
    writer.write(str(fn), magicc_version=7)

    magicc_flag = CONC_MAGICC_FLAG_MAP[openscm_runner_variable]
    if "__" in magicc_flag:
        key, index = magicc_flag.split("__")
        magicc_concentration_cfg[key][int(index)] = str(fn.relative_to(magicc_run_dir))

    else:
        magicc_concentration_cfg[magicc_flag] = str(fn.relative_to(magicc_run_dir))
    # break


for k, v in magicc_concentration_cfg.items():
    if isinstance(v, list):
        for vv in v:
            if vv is None:
                raise AssertionError(k)

    elif v is None:
        raise AssertionError(k)

# magicc_concentration_cfg

# %%
# magicc_concentration_cfg

# %% [markdown]
# ## Run MAGICC

# %% [markdown]
# ### Convert scenario to OpenSCM-Runner names

# %%
complete_openscm_runner = emissions.openscm.update_index_levels(
    {
        "variable": partial(
            convert_variable_name,
            from_convention=SupportedNamingConventions.GCAGES,
            to_convention=SupportedNamingConventions.OPENSCM_RUNNER,
        )
    },
)
# complete_openscm_runner

# %% [markdown]
# ### Configure

# %%
os.environ["MAGICC_EXECUTABLE_7"] = str(magicc_exe_p)

# %%
MAGICC_START_SCENARIO_YEAR = 2015

# %%
complete_openscm_runner_for_magicc = complete_openscm_runner.loc[:, MAGICC_START_SCENARIO_YEAR:]
# complete_openscm_runner_for_magicc

# %%
output_variables = (
    # GSAT
    "Surface Air Temperature Change",
    # # GMST
    "Surface Air Ocean Blended Temperature Change",
    # # ERFs
    "Effective Radiative Forcing",
    "Effective Radiative Forcing|Anthropogenic",
    "Effective Radiative Forcing|Aerosols",
    # "Effective Radiative Forcing|Aerosols|Direct Effect",
    # "Effective Radiative Forcing|Aerosols|Direct Effect|BC",
    # "Effective Radiative Forcing|Aerosols|Direct Effect|OC",
    # "Effective Radiative Forcing|Aerosols|Direct Effect|SOx",
    # "Effective Radiative Forcing|Aerosols|Indirect Effect",
    "Effective Radiative Forcing|Greenhouse Gases",
    "Effective Radiative Forcing|CO2",
    "Effective Radiative Forcing|CH4",
    "Effective Radiative Forcing|N2O",
    "Effective Radiative Forcing|F-Gases",
    "Effective Radiative Forcing|Montreal Protocol Halogen Gases",
    "Effective Radiative Forcing|Ozone",
    "Effective Radiative Forcing|Tropospheric Ozone",
    "Effective Radiative Forcing|Stratospheric Ozone",
    # TODO: consider updating solar and volcanic here too
    "Effective Radiative Forcing|Solar",
    "Effective Radiative Forcing|Volcanic",
    # # Heat uptake
    "Heat Uptake",
    "Heat Uptake|Ocean",
    # # Atmospheric concentrations
    "Atmospheric Concentrations|CO2",
    "Atmospheric Concentrations|CH4",
    "Atmospheric Concentrations|N2O",
    "Atmospheric Concentrations|C2F6",
    "Atmospheric Concentrations|C3F8",
    "Atmospheric Concentrations|C4F10",
    "Atmospheric Concentrations|C5F12",
    "Atmospheric Concentrations|C6F14",
    "Atmospheric Concentrations|C7F16",
    "Atmospheric Concentrations|C8F18",
    "Atmospheric Concentrations|cC4F8",
    "Atmospheric Concentrations|CF4",
    "Atmospheric Concentrations|CH2Cl2",
    "Atmospheric Concentrations|CHCl3",
    "Atmospheric Concentrations|HFC125",
    "Atmospheric Concentrations|HFC134a",
    "Atmospheric Concentrations|HFC143a",
    "Atmospheric Concentrations|HFC152a",
    "Atmospheric Concentrations|HFC227ea",
    "Atmospheric Concentrations|HFC23",
    "Atmospheric Concentrations|HFC236fa",
    "Atmospheric Concentrations|HFC245fa",
    "Atmospheric Concentrations|HFC32",
    "Atmospheric Concentrations|HFC365mfc",
    "Atmospheric Concentrations|HFC4310",
    "Atmospheric Concentrations|NF3",
    "Atmospheric Concentrations|SF6",
    "Atmospheric Concentrations|SO2F2",
    ## Prescribed conc gases for comparison
    "Atmospheric Concentrations|CCl4",
    "Atmospheric Concentrations|CFC11",
    "Atmospheric Concentrations|CFC12",
    "Atmospheric Concentrations|CFC113",
    "Atmospheric Concentrations|CFC114",
    "Atmospheric Concentrations|CFC115",
    "Atmospheric Concentrations|CH3Br",
    "Atmospheric Concentrations|CH3CCl3",
    "Atmospheric Concentrations|CH3Cl",
    "Atmospheric Concentrations|Halon1202",
    "Atmospheric Concentrations|Halon1211",
    "Atmospheric Concentrations|Halon1301",
    "Atmospheric Concentrations|Halon2402",
    "Atmospheric Concentrations|HCFC141b",
    "Atmospheric Concentrations|HCFC142b",
    "Atmospheric Concentrations|HCFC22",
    # # Carbon cycle
    # "Net Atmosphere to Land Flux|CO2",
    # "Net Atmosphere to Ocean Flux|CO2",
    # "CO2_CURRENT_NPP",
    # # Permafrost
    # "Net Land to Atmosphere Flux|CO2|Earth System Feedbacks|Permafrost",
    # "Net Land to Atmosphere Flux|CH4|Earth System Feedbacks|Permafrost",
    # "Sea Level Rise",
)


# %%
def load_magicc_cfgs(
    prob_distribution_path: Path,
    output_variables: tuple[str, ...],
    magicc_concentration_cfg: dict[str, Any],
    startyear: int = 1750,
) -> dict[str, list[dict[str, Any]]]:
    """
    Load MAGICC's configuration

    Parameters
    ----------
    prob_distribution_path
        Path to the file containing the probabilistic distribution

    output_variables
        Output variables

    startyear
        Starting year of the runs

    magicc_concentration_cfg
        Concentration configuration for MAGICC

    Returns
    -------
    :
        Config that can be used to run MAGICC
    """
    with open(prob_distribution_path) as fh:
        cfgs_raw = json.load(fh)

    cfgs_physical = [
        {
            "run_id": c["paraset_id"],
            **{k.lower(): v for k, v in c["nml_allcfgs"].items()},
        }
        for c in cfgs_raw["configurations"]
    ]

    common_cfg = {
        "startyear": startyear,
        # Note: endyear handled in gcages, which I don't love but is fine for now
        "out_dynamic_vars": convert_openscm_runner_output_names_to_magicc_output_names(output_variables),
        "out_ascii_binary": "BINARY",
        "out_binary_format": 2,
    }

    run_config = [{**common_cfg, **magicc_concentration_cfg, **physical_cfg} for physical_cfg in cfgs_physical]
    climate_models_cfgs = {"MAGICC7": run_config}

    return climate_models_cfgs


# %%
climate_models_cfgs = load_magicc_cfgs(
    prob_distribution_path=magicc_prob_distribution_p,
    output_variables=output_variables,
    magicc_concentration_cfg=magicc_concentration_cfg,
    startyear=1750,
)

# %%
badly_converted = [v for v in climate_models_cfgs["MAGICC7"][0]["out_dynamic_vars"] if v.upper() != v]
if badly_converted:
    raise AssertionError(badly_converted)

# %%
# climate_models_cfgs["MAGICC7"] = climate_models_cfgs["MAGICC7"][:10]

# %%
if magicc_version == "MAGICCv7.5.3" and platform.system() == "Darwin" and platform.processor() == "arm":
    os.environ["DYLD_LIBRARY_PATH"] = "/opt/homebrew/opt/gfortran/lib/gcc/current/"

# %% [markdown]
# ### Run

# %%
complete_openscm_runner_for_magicc.head(3)

# %%
# papermill_description=run-magicc
res = run_scms(
    scenarios=complete_openscm_runner_for_magicc,
    climate_models_cfgs=climate_models_cfgs,
    output_variables=output_variables,
    scenario_group_levels=["model", "scenario"],
    n_processes=n_magicc_workers,
    db=None,
    verbose=True,
    progress=True,
)
res = res.pix.assign(run_mode="concentration-driven")

# res

# %% [markdown]
# ## Check concentrations were prescribed correctly

# %%
for ghg in tqdm.auto.tqdm(concentrations_xr.data_vars):
    ghg_magicc = ghg.replace("hfc4310mee", "hfc4310")
    openscm_runner_variable = convert_magicc7_to_openscm_variables(f"{ghg_magicc}_conc".upper())

    np.testing.assert_allclose(
        np.broadcast_to(
            concentrations_xr[ghg].values, res.loc[pix.isin(variable=openscm_runner_variable), 2023:].shape
        ),
        res.loc[pix.isin(variable=openscm_runner_variable), 2023:].values,
        rtol=1e-4,
    )

# %% [markdown]
# ### Quick look plots

# %%
try:
    res_normal_mode = magicc_output_db.load(
        pix.isin(
            model=model,
            scenario=scenario,
            run_mode="magicc-concentration-to-emissions-switch",
            climate_model=res.pix.unique("climate_model"),
        )
        & pix.ismatch(
            variable=["*Concentrations**", "Surface Air Temperature Change", "Effective Radiative Forcing**"]
        ),
    )
    pdf = pix.concat([res, res_normal_mode])
except ValueError:
    pdf = res

# %%
pdf.loc[pix.isin(variable="Atmospheric Concentrations|CO2"), 2000:].openscm.plot_plume_after_calculating_quantiles(
    quantile_over="run_id",
    quantiles_plumes=((0.5, 0.9), ((1.0 / 4, 3.0 / 4), 0.7), ((1.0 / 6, 5.0 / 6), 0.5), ((0.05, 0.95), 0.2)),
    style_var="scenario",
    hue_var="run_mode",
)

# %%
for yrs in (range(2005, 2035 + 1), slice(None, None, None)):
    pdf.loc[pix.isin(variable="Atmospheric Concentrations|CH4"), yrs].openscm.plot_plume_after_calculating_quantiles(
        quantile_over="run_id",
        quantiles_plumes=((0.5, 0.9), ((1.0 / 4, 3.0 / 4), 0.7), ((1.0 / 6, 5.0 / 6), 0.5), ((0.05, 0.95), 0.2)),
        style_var="scenario",
        hue_var="run_mode",
    )
    plt.show()

# %%
pdf.loc[
    pix.isin(variable="Effective Radiative Forcing|Greenhouse Gases"), :
].openscm.plot_plume_after_calculating_quantiles(
    quantile_over="run_id",
    quantiles_plumes=((0.5, 0.9), ((1.0 / 4, 3.0 / 4), 0.7), ((1.0 / 6, 5.0 / 6), 0.5), ((0.05, 0.95), 0.2)),
    style_var="scenario",
    hue_var="run_mode",
)

# %%
pdf.loc[pix.isin(variable="Effective Radiative Forcing|CO2"), :].openscm.plot_plume_after_calculating_quantiles(
    quantile_over="run_id",
    quantiles_plumes=((0.5, 0.9), ((1.0 / 4, 3.0 / 4), 0.7), ((1.0 / 6, 5.0 / 6), 0.5), ((0.05, 0.95), 0.2)),
    style_var="scenario",
    hue_var="run_mode",
)

# %%
pdf.loc[pix.isin(variable="Effective Radiative Forcing|CH4"), :].openscm.plot_plume_after_calculating_quantiles(
    quantile_over="run_id",
    quantiles_plumes=((0.5, 0.9), ((1.0 / 4, 3.0 / 4), 0.7), ((1.0 / 6, 5.0 / 6), 0.5), ((0.05, 0.95), 0.2)),
    style_var="scenario",
    hue_var="run_mode",
)

# %%
pdf.loc[pix.isin(variable="Effective Radiative Forcing|Ozone"), :].openscm.plot_plume_after_calculating_quantiles(
    quantile_over="run_id",
    quantiles_plumes=((0.5, 0.9), ((1.0 / 4, 3.0 / 4), 0.7), ((1.0 / 6, 5.0 / 6), 0.5), ((0.05, 0.95), 0.2)),
    style_var="scenario",
    hue_var="run_mode",
)

# %%
pdf.loc[pix.isin(variable="Effective Radiative Forcing|N2O"), :].openscm.plot_plume_after_calculating_quantiles(
    quantile_over="run_id",
    quantiles_plumes=((0.5, 0.9), ((1.0 / 4, 3.0 / 4), 0.7), ((1.0 / 6, 5.0 / 6), 0.5), ((0.05, 0.95), 0.2)),
    style_var="scenario",
    hue_var="run_mode",
)

# %%
pdf.loc[
    pix.isin(variable="Effective Radiative Forcing|Montreal Protocol Halogen Gases"), :
].openscm.plot_plume_after_calculating_quantiles(
    quantile_over="run_id",
    quantiles_plumes=((0.5, 0.9), ((1.0 / 4, 3.0 / 4), 0.7), ((1.0 / 6, 5.0 / 6), 0.5), ((0.05, 0.95), 0.2)),
    style_var="scenario",
    hue_var="run_mode",
)

# %%
pdf.loc[pix.isin(variable="Effective Radiative Forcing|Aerosols"), :].openscm.plot_plume_after_calculating_quantiles(
    quantile_over="run_id",
    quantiles_plumes=((0.5, 0.9), ((1.0 / 4, 3.0 / 4), 0.7), ((1.0 / 6, 5.0 / 6), 0.5), ((0.05, 0.95), 0.2)),
    style_var="scenario",
    hue_var="run_mode",
)

# %%
pdf.loc[pix.isin(variable="Surface Air Temperature Change"), 2000:].openscm.plot_plume_after_calculating_quantiles(
    quantile_over="run_id",
    quantiles_plumes=((0.5, 0.9), ((1.0 / 4, 3.0 / 4), 0.7), ((1.0 / 6, 5.0 / 6), 0.5), ((0.05, 0.95), 0.2)),
    style_var="scenario",
    hue_var="run_mode",
)

# %%
n_run_modes_to_show = 2
if len(pdf.pix.unique("run_mode")) == n_run_modes_to_show:
    tmp = pdf.loc[pix.isin(variable="Surface Air Temperature Change")].openscm.groupby_except("run_id").median()
    ax = (
        tmp.loc[pix.isin(run_mode="concentration-driven")]
        .reset_index("run_mode", drop=True)
        .subtract(
            tmp.loc[pix.isin(run_mode="magicc-concentration-to-emissions-switch")].reset_index("run_mode", drop=True)
        )
        .pix.assign(run_mode="concentration-driven - magicc-concentration-to-emissions-switch")
        .pix.project(["scenario", "run_mode"])
        .T.plot()
    )
    ax.legend(loc="center left", bbox_to_anchor=(1.05, 0.5))
    ax.grid()
    plt.show()

    peak_warming = pdf.loc[pix.isin(variable="Surface Air Temperature Change")].max(axis="columns")
    pdf_box = peak_warming.to_frame("peak_warming").reset_index()
    ax = sns.boxplot(
        data=pdf_box,
        y="peak_warming",
        x="run_mode",
    )
    ax.set_yticks(
        np.arange(pdf_box["peak_warming"].min().round(1) - 0.1, pdf_box["peak_warming"].max().round(1) + 0.1, 0.05),
        minor=True,
    )
    ax.grid(which="minor")
    ax.grid(which="major", linewidth=2)
    plt.show()

    # Not adjusted to assessed warming hence can differ from 'normal' reporting
    display(peak_warming.groupby(["run_mode"]).describe().round(3))

# %% [markdown]
# ## Save to database

# %%
magicc_output_db.save(
    res,
    groupby=["climate_model", "model", "scenario", "variable", "run_mode"],
    allow_overwrite=True,
    warn_on_partial_overwrite=False,
    max_workers=n_magicc_workers,
    # progress=True,
)
