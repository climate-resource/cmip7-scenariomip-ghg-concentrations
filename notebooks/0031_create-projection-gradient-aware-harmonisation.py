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
# # Create annual-means for gases using gradient-aware harmonisation

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Imports

# %% editable=true slideshow={"slide_type": ""}
from pathlib import Path

import continuous_timeseries as ct
import matplotlib.pyplot as plt
import numpy as np
import openscm_units
import pandas as pd
import pandas_indexing as pix
import pandas_openscm
import pandas_openscm.db
import seaborn as sns
import tqdm.auto
import xarray as xr
from gradient_aware_harmonisation.add_cubic import (
    harmonise_splines_add_cubic,
)
from gradient_aware_harmonisation.spline import SplineScipy

from cmip7_scenariomip_ghg_generation.constants import GHG_LIFETIMES
from cmip7_scenariomip_ghg_generation.scenario_info import ScenarioInfo

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Parameters

# %% editable=true slideshow={"slide_type": ""} tags=["parameters"]
ghg: str = "ch4"
scenario_info_markers: str = (
    "WITCH 6.0;SSP5 - Medium-Low Emissions_a;hl;;"
    "REMIND-MAgPIE 3.5-4.10;SSP1 - Very Low Emissions;vllo;;"
    "MESSAGEix-GLOBIOM-GAINS 2.1-M-R12;SSP2 - Low Emissions;l;;"
    "IMAGE 3.4;SSP2 - Medium Emissions;m;;"
    "GCAM 7.1 scenarioMIP;SSP3 - High Emissions;h;;"
    "AIM 3.0;SSP2 - Low Overshoot;vlho;;"
    "COFFEE 1.6;SSP2 - Medium-Low Emissions;ml"
)
historical_data_root_dir: str = "../output-bundles/dev-test/data/raw/historical-ghg-concs"
magicc_output_db_dir: str = "../output-bundles/dev-test/data/interim/magicc-output/db"
magicc_db_backend_str: str = "feather"
out_file: str = (
    "../output-bundles/dev-test/data/interim/annual-means/gradient-aware-harmonisation_ch4_annual-mean.feather"
)


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
UR = openscm_units.unit_registry
Q = UR.Quantity
UR.setup_matplotlib(enable=True)

# %%
palette = {
    "vllo": "#24a4ff",
    "vlho": "#4a0daf",
    "l": "#00cc69",
    "ml": "#f5ac00",
    "m": "#ffa9dc",
    "h": "#700000",
    "hl": "#8f003b",
    "historical": "black",
}

scenario_order = ["vllo", "vlho", "l", "ml", "m", "hl", "h"]

# %% [markdown] editable=true slideshow={"slide_type": ""}
# ## Load data

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
magiccc_output_pdf = add_cmip_scenario_name(magiccc_output)
# magiccc_output_pdf

# %% [markdown]
# ## Gradient-aware harmonisation

# %% [markdown]
# ### Starting point


# %%
def to_timeseries(
    indf: pd.DataFrame,
    name: str,
    interpolation: ct.InterpolationOption,
    time_units: str = "yr",
    unit_level: str = "unit",
) -> ct.Timeseries:
    """
    Convert DataFrame to Timeseries
    """
    if indf.shape[0] != 1:
        raise AssertionError

    time_axis = Q(indf.columns.values, time_units)

    unit_l = indf.index.get_level_values(unit_level).unique()
    if len(unit_l) != 1:
        raise AssertionError(unit_l)
    unit = unit_l[0]

    values = Q(indf.values.squeeze(), unit)

    res = ct.Timeseries.from_arrays(
        x=time_axis,
        y=values,
        interpolation=interpolation,
        name=name,
    )

    return res


# %%
magiccc_output_pdf_median = magiccc_output_pdf.openscm.groupby_except("run_id").median()
# magiccc_output_pdf_median

# %%
historical_concs_ts = to_timeseries(
    historical_concs,
    name="historical",
    interpolation=ct.InterpolationOption.Linear,
)

scenarios_ts_d = {
    csn: to_timeseries(
        csn_df,
        name=csn,
        interpolation=ct.InterpolationOption.Linear,
    )
    for csn, csn_df in magiccc_output_pdf_median.groupby("cmip_scenario_name")
}

fig, ax = plt.subplots()
for ts in [historical_concs_ts, *scenarios_ts_d.values()]:
    ts.interpolate(
        ts.time_axis.bounds[np.where((ts.time_axis.bounds >= Q(2000, "yr")) & (ts.time_axis.bounds <= Q(2100, "yr")))]
    ).plot(ax=ax, continuous_plot_kwargs=dict(color=palette[ts.name]))

ax.legend()
ax.grid()
ax.set_title(ghg)

# %% [markdown]
# ### Harmonise

# %%
convergence_time_delta = {}
if ghg in GHG_LIFETIMES:
    convergence_time_delta[ghg] = GHG_LIFETIMES[ghg].to("yr").m * 3

# A few overrides
convergence_time_delta["ch4"] = 30 * 2
convergence_time_delta["co2"] = 15
convergence_time_delta["n2o"] = 75
convergence_time_delta

# %%
harmonisation_time = historical_concs_ts.time_axis.bounds.max().m
convergence_time = harmonisation_time + convergence_time_delta[ghg]

out_years = np.arange(harmonisation_time, magiccc_output_pdf_median.columns.max() + 1)

harmonised_l = []
for csn, ts in scenarios_ts_d.items():
    target = SplineScipy(historical_concs_ts.timeseries_continuous.function.ppoly)
    ts_scipy = SplineScipy(ts.timeseries_continuous.function.ppoly)

    harmonised_spline = harmonise_splines_add_cubic(
        diverge_from=target,
        harmonisee=ts_scipy,
        harmonisation_time=harmonisation_time,
        convergence_time=convergence_time,
    )

    tmp_df = pd.DataFrame(
        harmonised_spline(out_years)[np.newaxis, :],
        columns=out_years,
        index=magiccc_output_pdf_median.loc[pix.isin(cmip_scenario_name=csn)].index.droplevel(
            magiccc_output_pdf_median.index.names.difference(
                ["cmip_scenario_name", "model", "scenario", "region", "unit", "variable"]
            )
        ),
    )
    harmonised_l.append(tmp_df)

harmonised = pix.concat(harmonised_l)
# harmonised

# %% [markdown]
# ## Plot

# %%
fig, axes = plt.subplot_mosaic([["vllo", "vlho", "l"], ["ml", "m", "."], ["hl", "h", "."]], figsize=(20, 20))
for scenario, ax in axes.items():
    pdf = pix.concat(
        [
            historical_concs.pix.assign(cmip_scenario_name="historical", source="cmip-concs").reset_index(
                historical_concs.index.names.difference(["unit", "cmip_scenario_name", "source"]), drop=True
            ),
            magiccc_output_pdf_median.loc[pix.isin(cmip_scenario_name=scenario)]
            .pix.assign(source="MAGICC")
            .reset_index(
                magiccc_output_pdf_median.index.names.difference(["unit", "cmip_scenario_name", "source"]), drop=True
            ),
            harmonised.loc[pix.isin(cmip_scenario_name=scenario)]
            .pix.assign(source="harmonised")
            .reset_index(harmonised.index.names.difference(["unit", "cmip_scenario_name", "source"]), drop=True),
        ]
    ).loc[:, 2000:]

    sns.lineplot(
        data=pdf.openscm.to_long_data(),
        x="time",
        y="value",
        hue="cmip_scenario_name",
        palette=palette,
        style="source",
        ax=ax,
    )
    ax.set_ylim(ymin=historical_concs.min().min())

fig.suptitle(ghg)
plt.show()

# %% [markdown]
# ## Save

# %%
out = harmonised.pix.assign(ghg=ghg, scenario=harmonised.index.get_level_values("cmip_scenario_name")).pix.project(
    ["unit", "scenario", "ghg"]
)
out

# %%
out_file_p.parent.mkdir(exist_ok=True, parents=True)
out.to_feather(out_file_p)
