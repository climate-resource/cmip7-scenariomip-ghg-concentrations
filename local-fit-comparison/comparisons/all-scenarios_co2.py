# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.19.5
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # All scenarios: trends, seasonality and zonal means, CO2
#
# For every marker scenario (source ids `CR-<scenario>-1-1-0`), compares the
# baseline run with the local-fit run:
#
# - global-mean trends, monthly and yearly (one panel per run)
# - average seasonality over the whole period (one panel per run)
# - time-averaged zonal profile (latitude on the x-axis, one panel per run)

# %%
import glob

import matplotlib.pyplot as plt
import xarray as xr

# %% tags=["parameters"]
ghg: str = "co2"
scenario_codes: list[str] = ["vl", "ln", "l", "ml", "m", "hl", "h"]
baseline_run_id: str = "dev-test"
comparison_run_id: str = "local-fit-satweighted"
output_bundles_root: str = "../../output-bundles"

palette = {
    "vl": "#24a4ff",
    "ln": "#4a0daf",
    "l": "#00cc69",
    "ml": "#f5ac00",
    "m": "#ffa9dc",
    "h": "#700000",
    "hl": "#8f003b",
}
run_ids = {"baseline": baseline_run_id, "local": comparison_run_id}


# %% [markdown]
# ## Load


# %%
def load_var(run_id: str, scenario_code: str, grid: str, freq: str) -> xr.DataArray:
    """
    Load one grid/frequency of the ESGF-ready output for one run and scenario
    """
    source_id = f"CR-{scenario_code}-1-1-0"
    pattern = (
        f"{output_bundles_root}/{run_id}/data/processed/esgf-ready/input4MIPs/"
        f"CMIP7/ScenarioMIP/CR/{source_id}/atmos/{freq}/{ghg}/{grid}/v*/"
        f"{ghg}_input4MIPs_*_{grid}_*.nc"
    )
    return xr.open_dataset(glob.glob(pattern)[0])[ghg]


data = {
    (label, sc): {
        "gm_mon": load_var(run_id, sc, "gm", "mon"),
        "gm_yr": load_var(run_id, sc, "gm", "yr"),
        "gnz_mon": load_var(run_id, sc, "gnz", "mon"),
    }
    for label, run_id in run_ids.items()
    for sc in scenario_codes
}

units = data[("baseline", scenario_codes[0])]["gm_mon"].attrs.get("units", ghg)

# %% [markdown]
# ## Global-mean trends, monthly and yearly

# %%
for freq_key, freq_label in [("gm_mon", "monthly"), ("gm_yr", "yearly")]:
    fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(16, 5), sharey=True)
    for ax, label in zip(axes, run_ids):
        for sc in scenario_codes:
            data[(label, sc)][freq_key].plot(ax=ax, color=palette[sc], label=sc)
        ax.set_title(f"{ghg.upper()} - {label} ({run_ids[label]}) - {freq_label}")
        ax.set_ylabel(units)
        ax.legend()
        ax.grid()
    plt.tight_layout()
    plt.show()

# %% [markdown]
# ## Average seasonality over the whole period

# %%
fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(16, 5), sharey=True)
for ax, label in zip(axes, run_ids):
    for sc in scenario_codes:
        seasonality = data[(label, sc)]["gm_mon"].groupby("time.month").mean("time")
        seasonality.plot(ax=ax, color=palette[sc], label=sc)
    ax.set_title(f"{ghg.upper()} - {label} ({run_ids[label]}) - seasonality, whole period")
    ax.set_xlabel("Month")
    ax.set_ylabel(units)
    ax.legend()
    ax.grid()
plt.tight_layout()
plt.show()

# %% [markdown]
# ## Zonal profile, averaged over time

# %%
fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(16, 5), sharey=True)
for ax, label in zip(axes, run_ids):
    for sc in scenario_codes:
        data[(label, sc)]["gnz_mon"].mean("time").plot(ax=ax, color=palette[sc], label=sc)
    ax.set_title(f"{ghg.upper()} - {label} ({run_ids[label]}) - time-averaged zonal profile")
    ax.set_xlabel("Latitude")
    ax.set_ylabel(units)
    ax.legend()
    ax.grid()
plt.tight_layout()
plt.show()

# %% [markdown]
# ## Differences between runs (local - baseline)

# %%
fig, axes = plt.subplots(nrows=2, ncols=2, figsize=(16, 10))
ax_mon, ax_yr, ax_seas, ax_zonal = axes.flatten()

for sc in scenario_codes:
    base = data[("baseline", sc)]
    loc = data[("local", sc)]

    (loc["gm_mon"] - base["gm_mon"]).plot(ax=ax_mon, color=palette[sc], label=sc)
    (loc["gm_yr"] - base["gm_yr"]).plot(ax=ax_yr, color=palette[sc], label=sc)

    base_seas = base["gm_mon"].groupby("time.month").mean("time")
    loc_seas = loc["gm_mon"].groupby("time.month").mean("time")
    (loc_seas - base_seas).plot(ax=ax_seas, color=palette[sc], label=sc)

    (loc["gnz_mon"].mean("time") - base["gnz_mon"].mean("time")).plot(ax=ax_zonal, color=palette[sc], label=sc)

for ax, title in [
    (ax_mon, "global-mean trend, monthly"),
    (ax_yr, "global-mean trend, yearly"),
    (ax_seas, "seasonality, whole period"),
    (ax_zonal, "time-averaged zonal profile"),
]:
    ax.set_title(f"{ghg.upper()} - {comparison_run_id} minus {baseline_run_id} - {title}")
    ax.set_ylabel(units)
    ax.axhline(0.0, linestyle="--", color="gray")
    ax.legend()
    ax.grid()

ax_seas.set_xlabel("Month")
ax_zonal.set_xlabel("Latitude")
plt.tight_layout()
plt.show()

# %%

# %%
