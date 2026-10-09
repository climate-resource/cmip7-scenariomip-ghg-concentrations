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
# # Compare global trends: ml, CO2
#
# Opens the CO2 ESGF-ready output for the `ml` marker scenario
# (source id `CR-ml-1-1-0`) from two runs:
#
# - `baseline_run_id`: the baseline historical-data run
# - `comparison_run_id`: the run using a substituted local historical fit using satellite data

# %%
import glob

import matplotlib.pyplot as plt
import xarray as xr

# %% tags=["parameters"]
scenario_code: str = "ml"
ghg: str = "co2"
baseline_run_id: str = "dev-test"
comparison_run_id: str = "local-fit-satweighted"
output_bundles_root: str = "../../output-bundles"


# %% [markdown]
# ## Load


# %%
def load_gm(run_id: str, freq: str) -> xr.Dataset:
    """
    Load the pipeline's pre-computed global-mean (`gm`) file for one run/frequency
    """
    source_id = f"CR-{scenario_code}-1-1-0"
    pattern = (
        f"{output_bundles_root}/{run_id}/data/processed/esgf-ready/input4MIPs/"
        f"CMIP7/ScenarioMIP/CR/{source_id}/atmos/{freq}/{ghg}/gm/v*/{ghg}_input4MIPs_*_gm_*.nc"
    )
    return xr.open_dataset(glob.glob(pattern)[0])


def load_gnz(run_id: str) -> xr.Dataset:
    """
    Load the 15-degree zonal-mean (`gnz`) monthly file for one run
    """
    source_id = f"CR-{scenario_code}-1-1-0"
    pattern = (
        f"{output_bundles_root}/{run_id}/data/processed/esgf-ready/input4MIPs/"
        f"CMIP7/ScenarioMIP/CR/{source_id}/atmos/mon/{ghg}/gnz/v*/{ghg}_input4MIPs_*_gnz_*.nc"
    )
    return xr.open_dataset(glob.glob(pattern)[0])


# %%
baseline_gm_mon = load_gm(baseline_run_id, "mon")[ghg]
comparison_gm_mon = load_gm(comparison_run_id, "mon")[ghg]
baseline_gm_yr = load_gm(baseline_run_id, "yr")[ghg]
comparison_gm_yr = load_gm(comparison_run_id, "yr")[ghg]

baseline_gnz_mon = load_gnz(baseline_run_id)[ghg]
comparison_gnz_mon = load_gnz(comparison_run_id)[ghg]
baseline_gnz_yr = baseline_gnz_mon.resample(time="YS").mean()
comparison_gnz_yr = comparison_gnz_mon.resample(time="YS").mean()

# %% [markdown]
# ## Global mean over time

# %%
for baseline, comparison, freq_label in [
    (baseline_gm_mon, comparison_gm_mon, "monthly"),
    (baseline_gm_yr, comparison_gm_yr, "yearly"),
]:
    fig, ax = plt.subplots(figsize=(10, 4))
    baseline.plot(ax=ax, label=baseline_run_id)
    comparison.plot(ax=ax, label=comparison_run_id)
    ax.set_title(f"{ghg.upper()} - scenario {scenario_code} - {freq_label}")
    ax.set_ylabel(baseline.attrs.get("units", ghg))
    ax.legend()
    ax.grid()
    plt.show()

# %% [markdown]
# ## Difference (comparison - baseline)

# %%
for baseline, comparison, freq_label in [
    (baseline_gm_mon, comparison_gm_mon, "monthly"),
    (baseline_gm_yr, comparison_gm_yr, "yearly"),
]:
    fig, ax = plt.subplots(figsize=(10, 4))
    diff = comparison - baseline
    diff.plot(ax=ax)
    ax.set_title(f"{ghg.upper()} - scenario {scenario_code} - {freq_label} - {comparison_run_id} minus {baseline_run_id}")
    ax.set_ylabel(baseline.attrs.get("units", ghg))
    ax.axhline(0.0, linestyle="--", color="gray")
    ax.grid()
    plt.show()

# %% [markdown]
# ## Zonal trend over time

# %%
for baseline_da, comparison_da, freq_label in [
    (baseline_gnz_mon, comparison_gnz_mon, "monthly"),
    (baseline_gnz_yr, comparison_gnz_yr, "yearly"),
]:
    diff_da = comparison_da - baseline_da

    fig, axes = plt.subplots(nrows=3, ncols=1, figsize=(10, 9), sharex=True)

    baseline_da.plot(ax=axes[0], x="time", y="lat")
    axes[0].set_title(f"{ghg.upper()} - scenario {scenario_code} - {freq_label} - {baseline_run_id}")

    comparison_da.plot(ax=axes[1], x="time", y="lat")
    axes[1].set_title(f"{ghg.upper()} - scenario {scenario_code} - {freq_label} - {comparison_run_id}")

    diff_da.plot(ax=axes[2], x="time", y="lat", cmap="RdBu_r")
    axes[2].set_title(
        f"{ghg.upper()} - scenario {scenario_code} - {freq_label} - {comparison_run_id} minus {baseline_run_id}"
    )

    for ax in axes:
        ax.set_xlabel("")

    axes[-1].set_xlabel("Time")
    plt.tight_layout()
    plt.show()

# %% [markdown]
# ## Seasonality
#
# Monthly climatology (average by calendar month) of the global mean,
# using the `gm` monthly data only.

# %%
baseline_seasonality = baseline_gm_mon.groupby("time.month").mean("time")
comparison_seasonality = comparison_gm_mon.groupby("time.month").mean("time")

fig, ax = plt.subplots(figsize=(8, 4))
baseline_seasonality.plot(ax=ax, label=baseline_run_id)
comparison_seasonality.plot(ax=ax, label=comparison_run_id)
ax.set_title(f"{ghg.upper()} - scenario {scenario_code} - monthly climatology, whole period")
ax.set_xlabel("Month")
ax.set_ylabel(baseline_gm_mon.attrs.get("units", ghg))
ax.legend()
ax.grid()
plt.show()

# %%
diff_seasonality = comparison_seasonality - baseline_seasonality

fig, ax = plt.subplots(figsize=(8, 4))
diff_seasonality.plot(ax=ax)
ax.set_title(f"{ghg.upper()} - scenario {scenario_code} - monthly climatology diff, whole period")
ax.set_xlabel("Month")
ax.set_ylabel(baseline_gm_mon.attrs.get("units", ghg))
ax.axhline(0.0, linestyle="--", color="gray")
ax.grid()
plt.show()

# %% [markdown]
# ## Seasonality diff, by 25-year chunk
#
# Same monthly climatology as above, but computed separately for each
# consecutive 25-year chunk of the record. Left column: the diff
# (comparison - baseline). Right column: both runs' seasonality over the same chunk.

# %%
start_year = int(baseline_gm_mon["time"].dt.year.min())
end_year = int(baseline_gm_mon["time"].dt.year.max())
chunk_starts = list(range(start_year, end_year + 1, 25))

fig, axes = plt.subplots(nrows=len(chunk_starts), ncols=2, figsize=(14, 3 * len(chunk_starts)), sharex=True)

for (ax_diff, ax_seas), chunk_start in zip(axes, chunk_starts):
    chunk_end = chunk_start + 24
    period = f"{chunk_start}-{min(chunk_end, end_year)}"

    baseline_chunk_seasonality = baseline_gm_mon.sel(time=slice(str(chunk_start), str(chunk_end))).groupby(
        "time.month"
    ).mean("time")
    comparison_chunk_seasonality = comparison_gm_mon.sel(time=slice(str(chunk_start), str(chunk_end))).groupby(
        "time.month"
    ).mean("time")

    (comparison_chunk_seasonality - baseline_chunk_seasonality).plot(ax=ax_diff)
    ax_diff.set_title(f"{ghg.upper()} - scenario {scenario_code} - {period} - diff")
    ax_diff.axhline(0.0, linestyle="--", color="gray")
    ax_diff.grid()

    baseline_chunk_seasonality.plot(ax=ax_seas, label=baseline_run_id)
    comparison_chunk_seasonality.plot(ax=ax_seas, label=comparison_run_id)
    ax_seas.set_title(f"{ghg.upper()} - scenario {scenario_code} - {period} - seasonality")
    ax_seas.set_ylabel(baseline_gm_mon.attrs.get("units", ghg))
    ax_seas.legend()
    ax_seas.grid()

diff_ymin = min(ax.get_ylim()[0] for ax in axes[:, 0])
diff_ymax = max(ax.get_ylim()[1] for ax in axes[:, 0])
for ax in axes[:, 0]:
    ax.set_ylim(diff_ymin, diff_ymax)

for ax in axes[-1]:
    ax.set_xlabel("Month")
plt.tight_layout()
plt.show()

# %% [markdown]
# ## Winter vs summer trends
#
# DJF and JJA quarterly means over time

# %%
baseline_quarterly = baseline_gm_mon.resample(time="QS-DEC").mean()
comparison_quarterly = comparison_gm_mon.resample(time="QS-DEC").mean()

baseline_djf = baseline_quarterly.sel(time=baseline_quarterly.time.dt.month == 12)
baseline_jja = baseline_quarterly.sel(time=baseline_quarterly.time.dt.month == 6)
comparison_djf = comparison_quarterly.sel(time=comparison_quarterly.time.dt.month == 12)
comparison_jja = comparison_quarterly.sel(time=comparison_quarterly.time.dt.month == 6)

fig, axes = plt.subplots(nrows=2, ncols=2, figsize=(14, 8), sharex=True)

baseline_djf.plot(ax=axes[0, 0], label=baseline_run_id)
comparison_djf.plot(ax=axes[0, 0], label=comparison_run_id)
axes[0, 0].set_title(f"{ghg.upper()} - scenario {scenario_code} - DJF")
axes[0, 0].legend()
axes[0, 0].grid()

baseline_jja.plot(ax=axes[0, 1], label=baseline_run_id)
comparison_jja.plot(ax=axes[0, 1], label=comparison_run_id)
axes[0, 1].set_title(f"{ghg.upper()} - scenario {scenario_code} - JJA")
axes[0, 1].legend()
axes[0, 1].grid()

(comparison_djf - baseline_djf).plot(ax=axes[1, 0])
axes[1, 0].set_title(f"DJF - {comparison_run_id} minus {baseline_run_id}")
axes[1, 0].axhline(0.0, linestyle="--", color="gray")
axes[1, 0].grid()

(comparison_jja - baseline_jja).plot(ax=axes[1, 1])
axes[1, 1].set_title(f"JJA - {comparison_run_id} minus {baseline_run_id}")
axes[1, 1].axhline(0.0, linestyle="--", color="gray")
axes[1, 1].grid()

for ax in axes.flatten():
    ax.set_ylabel(baseline_gm_mon.attrs.get("units", ghg))
    ax.set_xlabel("")

axes[1, 0].set_xlabel("Time")
axes[1, 1].set_xlabel("Time")
plt.tight_layout()
plt.show()

# %%
