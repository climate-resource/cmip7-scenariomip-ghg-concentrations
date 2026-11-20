"""
Tools for comparing two sets of ESGF-ready GHG concentration files

Used by the `compare_<a>_vs_<b>.py` notebooks in this folder.

A "bundle" is either a run's output folder (`output-bundles/<run-id>`)
or any folder that contains ESGF-ready netCDF files.
Files are paired by filename, so the folder layout inside a bundle does not matter
(e.g. the `vYYYYMMDD` version folders can differ between the two sides).
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import cftime
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pooch
import requests
import tqdm.auto
import xarray as xr

REPO_ROOT = Path(__file__).resolve().parents[1]
"""Root of the repository (useful for building bundle paths in notebooks)"""

DEFAULT_IGNORED_ATTRS: tuple[str, ...] = ("creation_date", "tracking_id", "doi", "amip")
"""
Attributes that are expected to differ and are therefore not reported as differences

- `creation_date`, `tracking_id`: new on every run
- `doi`: placeholder (`no-zenodo-token`) unless a Zenodo token is used
- `amip`: coordinate attribute added by the ESGF publication step
"""

ESGF_SEARCH_URL = "https://esgf-node.ornl.gov/esgf-1-5-bridge"
"""ESGF search index used to find files"""

ESGF_DATA_NODE = "esgf-node.ornl.gov"
"""ESGF data node to download from (each file is also replicated on other nodes)"""

ESGF_FILE_LIST_NAME = "esgf-file-list.json"
"""Name of the file (in the mirror bundle) in which the ESGF file list is saved"""

SCENARIO_ORDER = ("vl", "ln", "l", "ml", "m", "hl", "h")
"""Order in which scenarios are shown (others are appended alphabetically)"""

GRID_ORDER = ("gm", "gnz", "gr1z")
"""Order in which grids are shown (others are appended alphabetically)"""

STATUS_IDENTICAL = "identical"
STATUS_WITHIN_NOISE = "within noise"
STATUS_ABOVE_NOISE = "above noise"
STATUS_STRUCTURE = "structure differs"
STATUSES = (STATUS_IDENTICAL, STATUS_WITHIN_NOISE, STATUS_ABOVE_NOISE, STATUS_STRUCTURE)
"""All possible statuses of a compared file"""

_N_DIGITS_YEAR = 4
_N_DIGITS_YEAR_MONTH = 6


@dataclass(frozen=True)
class NoiseRule:
    """
    Rule for deciding whether a difference is just numerical (rounding) noise

    A difference between A and B at a point counts as noise if

        |A - B| <= max(float32_steps * spacing, abs_floor_fraction * gas_scale)

    where

    - spacing is the gap between adjacent float32 numbers at max(|A|, |B|)
      (the files store float32, so this is the smallest difference they can show)
    - gas_scale is the largest absolute value of that gas across all compared files
      (this stops tiny differences at near-zero concentrations being flagged)

    A difference of exactly zero is always "identical", never "noise".
    """

    float32_steps: float = 8
    """Number of float32 steps (at the value's magnitude) treated as noise"""

    abs_floor_fraction: float = 1e-6
    """Absolute floor, as a fraction of the gas's largest value, below which differences are noise"""

    def thresholds(self, magnitude: np.ndarray, gas_scale: float) -> np.ndarray:
        """
        Get the noise threshold at each point

        Parameters
        ----------
        magnitude
            max(|A|, |B|) at each point

        gas_scale
            Largest absolute value of the gas across all compared files

        Returns
        -------
        :
            Noise threshold at each point (same shape as `magnitude`)
        """
        steps = self.float32_steps * float32_spacing(magnitude)
        floor = self.abs_floor_fraction * gas_scale

        return np.maximum(steps, floor)

    def describe(self) -> str:
        """
        Describe the rule in words

        Returns
        -------
        :
            Human-readable description
        """
        return (
            "A difference counts as numerical noise if\n"
            f"  |A - B| <= max({self.float32_steps:g} float32 steps at that value, "
            f"{self.abs_floor_fraction:g} x the gas's largest value)\n"
            "A difference of exactly zero is 'identical'. Anything else is 'above noise'."
        )


@dataclass
class ComparisonResult:
    """
    Result of comparing two bundles
    """

    bundle_a: Path
    """First bundle"""

    bundle_b: Path
    """Second bundle"""

    noise_rule: NoiseRule
    """Noise rule used"""

    ignored_attrs: tuple[str, ...]
    """Attributes that were not compared"""

    files: pd.DataFrame
    """One row per file present in both bundles"""

    attribute_differences: pd.DataFrame
    """One row per (file, variable, attribute) that differs (excluding `ignored_attrs`)"""

    only_in_a: tuple[str, ...]
    """Filenames only found in bundle A"""

    only_in_b: tuple[str, ...]
    """Filenames only found in bundle B"""

    gas_scales: dict[str, float]
    """Largest absolute value of each gas across all compared files (used by the noise rule)"""

    paths_a: dict[str, Path]
    """Filename to path for bundle A"""

    paths_b: dict[str, Path]
    """Filename to path for bundle B"""


def float32_spacing(values: np.ndarray) -> np.ndarray:
    """
    Get the gap between adjacent float32 numbers at the given values

    Parameters
    ----------
    values
        Values

    Returns
    -------
    :
        Spacing (as float64)
    """
    return np.spacing(np.abs(np.asarray(values)).astype(np.float32)).astype(np.float64)


def esgf_ready_root(bundle: Path) -> Path:
    """
    Get the folder that holds the ESGF-ready files of a bundle

    Parameters
    ----------
    bundle
        A run's output folder or any folder of ESGF-ready files

    Returns
    -------
    :
        `bundle/data/processed/esgf-ready` if it exists, otherwise `bundle`
    """
    run_layout = bundle / "data" / "processed" / "esgf-ready"
    if run_layout.is_dir():
        return run_layout

    return bundle


def find_files(bundle: Path) -> dict[str, Path]:
    """
    Find all netCDF files in a bundle

    Parameters
    ----------
    bundle
        Bundle to search

    Returns
    -------
    :
        Filename to path

    Raises
    ------
    FileNotFoundError
        The bundle does not exist

    ValueError
        The same filename appears more than once (so pairing by name is ambiguous)
    """
    if not bundle.exists():
        msg = f"Bundle does not exist: {bundle}"
        raise FileNotFoundError(msg)

    out: dict[str, Path] = {}
    for fp in sorted(esgf_ready_root(bundle).rglob("*.nc")):
        if fp.name in out:
            msg = f"{fp.name} appears more than once in {bundle}: {out[fp.name]} and {fp}"
            raise ValueError(msg)

        out[fp.name] = fp

    return out


def parse_filename(name: str) -> dict[str, Any]:
    """
    Get the information encoded in an input4MIPs filename

    Expected form: `<variable>_input4MIPs_<category>_<mip>_<source_id>_<grid>_<start>-<end>.nc`

    Parameters
    ----------
    name
        Filename

    Returns
    -------
    :
        Variable, source ID, scenario, whether it is an extension,
        grid, frequency and period (`YYYY-YYYY`)
    """
    parts = name.removesuffix(".nc").split("_")
    variable, source_id, grid, time_range = parts[0], parts[-3], parts[-2], parts[-1]
    start, end = time_range.split("-")

    # Source IDs look like CR-vl-1-1-0 or CR-vl-ext-1-1-0
    source_id_parts = source_id.split("-")
    is_extension = "ext" in source_id_parts
    scenario_parts = [p for p in source_id_parts[1:] if p != "ext" and not p.isdigit()]
    scenario = "-".join(scenario_parts) if scenario_parts else source_id

    if len(start) == _N_DIGITS_YEAR:
        frequency = "yr"
    elif len(start) == _N_DIGITS_YEAR_MONTH:
        frequency = "mon"
    else:
        frequency = "unknown"

    return {
        "variable": variable,
        "source_id": source_id,
        "scenario": scenario,
        "extension": is_extension,
        "grid": grid,
        "frequency": frequency,
        "period": f"{start[:4]}-{end[:4]}",
        "start_year": int(start[:4]),
    }


def _years(time: xr.DataArray) -> np.ndarray:
    """Get the year of each (undecoded) time value"""
    dates = cftime.num2date(time.values, time.attrs["units"], time.attrs.get("calendar", "standard"))

    return np.array([d.year for d in np.atleast_1d(dates)])


def _decimal_years(time: xr.DataArray) -> np.ndarray:
    """Get (approximate) decimal years of each (undecoded) time value, for plotting"""
    dates = cftime.num2date(time.values, time.attrs["units"], time.attrs.get("calendar", "standard"))

    return np.array([d.year + (d.dayofyr - 1) / 365.25 for d in np.atleast_1d(dates)])


def _attrs_equal(a: Any, b: Any) -> bool:
    """Check whether two attribute values are equal (handles numpy arrays)"""
    if isinstance(a, np.ndarray) or isinstance(b, np.ndarray):
        return bool(np.array_equal(np.asarray(a), np.asarray(b)))

    return bool(a == b)


def _attribute_differences(
    attrs_a: dict[str, Any], attrs_b: dict[str, Any], ignored_attrs: Iterable[str]
) -> list[tuple[str, str, str]]:
    """Get (attribute, value in A, value in B) for every attribute that differs"""
    missing = "<missing>"
    out = []
    for key in sorted((set(attrs_a) | set(attrs_b)) - set(ignored_attrs)):
        value_a = attrs_a.get(key, missing)
        value_b = attrs_b.get(key, missing)
        if not _attrs_equal(value_a, value_b):
            out.append((key, str(value_a), str(value_b)))

    return out


def _load(fp: Path) -> xr.Dataset:
    """Load a file fully into memory, without decoding times"""
    with xr.open_dataset(fp, decode_times=False) as ds:
        return ds.load()


def _compare_file(  # noqa: PLR0913
    name: str,
    ds_a: xr.Dataset,
    ds_b: xr.Dataset,
    gas_scale: float,
    noise_rule: NoiseRule,
    ignored_attrs: tuple[str, ...],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """
    Compare one pair of files

    Returns the row for the files table and the rows for the attribute differences table.
    """
    info = parse_filename(name)
    variable = info["variable"]
    row: dict[str, Any] = {"file": name, **info}

    attr_rows = [
        {"file": name, "where": "global", "attribute": k, "value_a": va, "value_b": vb}
        for k, va, vb in _attribute_differences(ds_a.attrs, ds_b.attrs, ignored_attrs)
    ]
    for var_name in sorted(set(ds_a.variables) & set(ds_b.variables)):
        attr_rows.extend(
            {"file": name, "where": var_name, "attribute": k, "value_a": va, "value_b": vb}
            for k, va, vb in _attribute_differences(ds_a[var_name].attrs, ds_b[var_name].attrs, ignored_attrs)
        )

    # Structure: same variables, dimensions, dtypes and identical coordinates/bounds
    structure_issues = []
    if set(ds_a.variables) != set(ds_b.variables):
        structure_issues.append(
            f"variables differ (only in A: {sorted(set(ds_a.variables) - set(ds_b.variables))}, "
            f"only in B: {sorted(set(ds_b.variables) - set(ds_a.variables))})"
        )

    for var_name in sorted(set(ds_a.variables) & set(ds_b.variables)):
        va, vb = ds_a[var_name], ds_b[var_name]
        if va.dims != vb.dims or va.shape != vb.shape:
            structure_issues.append(f"{var_name}: dims {va.dims}{va.shape} vs {vb.dims}{vb.shape}")
        elif va.dtype != vb.dtype:
            structure_issues.append(f"{var_name}: dtype {va.dtype} vs {vb.dtype}")
        elif var_name != variable and not np.array_equal(va.values, vb.values):
            structure_issues.append(f"{var_name}: values differ")

    row["units"] = ds_b[variable].attrs.get("units") if variable in ds_b else None
    row["structure_issues"] = "; ".join(structure_issues)
    if variable not in ds_a or variable not in ds_b or ds_a[variable].shape != ds_b[variable].shape:
        row["status"] = STATUS_STRUCTURE
        return row, attr_rows

    a = ds_a[variable].values.astype(np.float64)
    b = ds_b[variable].values.astype(np.float64)
    nan_a, nan_b = np.isnan(a), np.isnan(b)
    abs_diff = np.where(nan_a & nan_b, 0.0, np.abs(a - b))
    # NaN on only one side counts as an (infinitely large) difference
    abs_diff = np.where(nan_a ^ nan_b, np.inf, abs_diff)

    magnitude = np.fmax(np.abs(a), np.abs(b))
    thresholds = noise_rule.thresholds(magnitude, gas_scale)
    differs = abs_diff > 0
    above_noise = abs_diff > thresholds

    years = _years(ds_a["time"])
    n_time = years.size

    def first_year(mask: np.ndarray) -> float:
        any_per_time = mask.reshape(n_time, -1).any(axis=1)
        return float(years[any_per_time].min()) if any_per_time.any() else np.nan

    with np.errstate(divide="ignore", invalid="ignore"):
        rel_diff = np.where(differs, abs_diff / np.abs(b), 0.0)
        steps = np.where(differs, abs_diff / float32_spacing(magnitude), 0.0)

    row.update(
        n_points=int(a.size),
        n_differ=int(differs.sum()),
        n_above_noise=int(above_noise.sum()),
        max_abs_diff=float(abs_diff.max()) if a.size else 0.0,
        max_rel_diff=float(np.nanmax(rel_diff)) if a.size else 0.0,
        max_float32_steps=float(np.nanmax(steps)) if a.size else 0.0,
        first_year_differ=first_year(differs),
        first_year_above_noise=first_year(above_noise),
    )
    if structure_issues:
        row["status"] = STATUS_STRUCTURE
    elif above_noise.any():
        row["status"] = STATUS_ABOVE_NOISE
    elif differs.any():
        row["status"] = STATUS_WITHIN_NOISE
    else:
        row["status"] = STATUS_IDENTICAL

    return row, attr_rows


def compare_bundles(
    bundle_a: Path,
    bundle_b: Path,
    noise_rule: NoiseRule = NoiseRule(),
    ignored_attrs: Iterable[str] = DEFAULT_IGNORED_ATTRS,
    gases: Iterable[str] | None = None,
) -> ComparisonResult:
    """
    Compare all files that two bundles have in common

    Parameters
    ----------
    bundle_a
        First bundle

    bundle_b
        Second bundle

    noise_rule
        Rule for deciding whether a difference is numerical noise

    ignored_attrs
        Attributes not to compare

    gases
        If supplied, only compare these gases

    Returns
    -------
    :
        Comparison result
    """
    ignored_attrs = tuple(ignored_attrs)
    paths_a = find_files(bundle_a)
    paths_b = find_files(bundle_b)
    if gases is not None:
        keep = set(gases)
        paths_a = {k: v for k, v in paths_a.items() if parse_filename(k)["variable"] in keep}
        paths_b = {k: v for k, v in paths_b.items() if parse_filename(k)["variable"] in keep}

    common = sorted(set(paths_a) & set(paths_b))
    by_gas: dict[str, list[str]] = {}
    for name in common:
        by_gas.setdefault(parse_filename(name)["variable"], []).append(name)

    rows = []
    attr_rows = []
    gas_scales = {}
    for gas, names in tqdm.auto.tqdm(sorted(by_gas.items()), desc="Gases"):
        loaded = {name: (_load(paths_a[name]), _load(paths_b[name])) for name in names}
        gas_scales[gas] = float(
            max(
                max(np.nanmax(np.abs(ds[gas].values)) if gas in ds and ds[gas].size else 0.0 for ds in pair)
                for pair in loaded.values()
            )
        )
        for name, (ds_a, ds_b) in loaded.items():
            row, file_attr_rows = _compare_file(name, ds_a, ds_b, gas_scales[gas], noise_rule, ignored_attrs)
            rows.append(row)
            attr_rows.extend(file_attr_rows)

    files = pd.DataFrame(rows)
    attribute_differences = pd.DataFrame(attr_rows, columns=["file", "where", "attribute", "value_a", "value_b"])

    return ComparisonResult(
        bundle_a=bundle_a,
        bundle_b=bundle_b,
        noise_rule=noise_rule,
        ignored_attrs=ignored_attrs,
        files=files,
        attribute_differences=attribute_differences,
        only_in_a=tuple(sorted(set(paths_a) - set(paths_b))),
        only_in_b=tuple(sorted(set(paths_b) - set(paths_a))),
        gas_scales=gas_scales,
        paths_a=paths_a,
        paths_b=paths_b,
    )


def values_verdict(result: ComparisonResult) -> str:
    """
    Get the verdict on the data values

    Parameters
    ----------
    result
        Comparison result

    Returns
    -------
    :
        One of "NOTHING COMPARED", "IDENTICAL", "DIFFERENCES WITHIN NOISE", "DIFFERENCES ABOVE NOISE"
    """
    if result.files.empty:
        return "NOTHING COMPARED"

    status = result.files["status"]
    if (status == STATUS_ABOVE_NOISE).any():
        return "DIFFERENCES ABOVE NOISE"

    if (status == STATUS_WITHIN_NOISE).any():
        return "DIFFERENCES WITHIN NOISE"

    return "IDENTICAL"


def print_verdict(result: ComparisonResult) -> None:
    """
    Print the overall verdict of a comparison

    Parameters
    ----------
    result
        Comparison result
    """
    files = result.files
    status = files["status"] if not files.empty else pd.Series(dtype=str)
    n_struct = int((status == STATUS_STRUCTURE).sum())
    values = values_verdict(result)
    structure_ok = (
        not result.only_in_a and not result.only_in_b and n_struct == 0 and result.attribute_differences.empty
    )
    no_change = values == "IDENTICAL" and structure_ok

    line = "=" * 80
    print(line)
    print(f"A: {result.bundle_a}")
    print(f"B: {result.bundle_b}")
    print(line)
    if no_change:
        print("NO CHANGE: every value in every file is bit-for-bit identical,")
        print("           the same files exist on both sides, and coordinates and metadata are identical")
        print(f"           (metadata ignored: {', '.join(result.ignored_attrs)})")
    else:
        print("CHANGES FOUND")
    print(line)

    print(f"Data values: {values}")
    for s in STATUSES:
        print(f"  {s:>17}: {int((status == s).sum())} files")

    print(f"Files only in A: {len(result.only_in_a)}")
    print(f"Files only in B: {len(result.only_in_b)}")
    print(f"Files with structure differences (variables, dims, dtypes, coordinates): {n_struct}")
    print(
        f"Attribute differences (ignoring {', '.join(result.ignored_attrs)}): "
        f"{len(result.attribute_differences)} in {result.attribute_differences['file'].nunique()} files"
    )
    print(line)


def summarise(result: ComparisonResult, by: Iterable[str] = ("variable",)) -> pd.DataFrame:
    """
    Summarise the per-file results

    Parameters
    ----------
    result
        Comparison result

    by
        Columns to group by (e.g. `("variable", "scenario", "period")`)

    Returns
    -------
    :
        Number of files with each status, largest differences
        and first year with a difference above noise, per group
    """
    by = list(by)
    files = result.files
    counts = (
        files.groupby(by)["status"].value_counts().unstack(fill_value=0).reindex(columns=list(STATUSES), fill_value=0)
    )
    stats = files.groupby(by).agg(
        units=("units", "first"),
        max_abs_diff=("max_abs_diff", "max"),
        max_rel_diff=("max_rel_diff", "max"),
        max_float32_steps=("max_float32_steps", "max"),
        first_year_above_noise=("first_year_above_noise", "min"),
    )

    return counts.join(stats)


def gases_above_noise(result: ComparisonResult) -> list[str]:
    """
    Get the gases with at least one difference above noise

    Parameters
    ----------
    result
        Comparison result

    Returns
    -------
    :
        Gases, sorted
    """
    files = result.files
    if files.empty:
        return []

    return sorted(files.loc[files["status"] == STATUS_ABOVE_NOISE, "variable"].unique())


def _ordered(values: Iterable[str], order: Iterable[str]) -> list[str]:
    """Sort `values` by `order`, appending unknown values alphabetically"""
    values = set(values)
    order = list(order)
    return [v for v in order if v in values] + sorted(values - set(order))


def _load_concatenated(paths: list[Path], variable: str) -> tuple[np.ndarray, np.ndarray | None, np.ndarray]:
    """Load and join (along time) a variable from several files, returning decimal years, latitudes and values"""
    datasets = sorted((_load(p) for p in paths), key=lambda ds: float(ds["time"].values[0]))
    times = np.concatenate([_decimal_years(ds["time"]) for ds in datasets])
    values = np.concatenate([ds[variable].values.astype(np.float64) for ds in datasets], axis=0)
    lat = datasets[0]["lat"].values if "lat" in datasets[0][variable].dims else None

    return times, lat, values


def plot_gas_differences(
    result: ComparisonResult,
    gas: str,
    frequency: str = "mon",
    mask_within_noise: bool = True,
) -> plt.Figure:
    """
    Plot the differences (A - B) for one gas

    One row per scenario, one column per grid.
    Main and extension periods are joined into one time axis.
    Global means are shown as lines with the noise band shaded in grey.
    Zonal grids are shown as latitude-time heatmaps.

    Parameters
    ----------
    result
        Comparison result

    gas
        Gas to plot

    frequency
        Frequency of the files to plot (`"mon"` or `"yr"`)

    mask_within_noise
        In the heatmaps, hide (show as white) differences that are within noise

    Returns
    -------
    :
        Figure
    """
    files = result.files
    rows = files[(files["variable"] == gas) & (files["frequency"] == frequency)]
    if rows.empty:
        msg = f"No {frequency} files for {gas}"
        raise ValueError(msg)

    scenarios = _ordered(rows["scenario"].unique(), SCENARIO_ORDER)
    grids = _ordered(rows["grid"].unique(), GRID_ORDER)
    gas_scale = result.gas_scales[gas]

    data = {}
    for (scenario, grid), group in rows.groupby(["scenario", "grid"]):
        names = list(group["file"])
        times, lat, values_a = _load_concatenated([result.paths_a[n] for n in names], gas)
        _, _, values_b = _load_concatenated([result.paths_b[n] for n in names], gas)
        diff = values_a - values_b
        thresholds = result.noise_rule.thresholds(np.fmax(np.abs(values_a), np.abs(values_b)), gas_scale)
        data[(scenario, grid)] = (times, lat, diff, thresholds)

    vmax = {}
    for grid in grids:
        shown = [
            np.where(np.abs(d) > t, d, 0.0) if mask_within_noise else d
            for (s, g), (_, _, d, t) in data.items()
            if g == grid
        ]
        vmax[grid] = max(float(np.nanmax(np.abs(d))) for d in shown) or 1.0

    fig, axes = plt.subplots(
        len(scenarios),
        len(grids),
        figsize=(5 * len(grids), 1.8 * len(scenarios) + 1),
        sharex=True,
        squeeze=False,
        constrained_layout=True,
    )
    units = rows["units"].iloc[0]
    for i, scenario in enumerate(scenarios):
        for j, grid in enumerate(grids):
            ax = axes[i, j]
            if (scenario, grid) not in data:
                ax.set_visible(False)
                continue

            times, lat, diff, thresholds = data[(scenario, grid)]
            if lat is None:
                ax.fill_between(times, -thresholds, thresholds, color="0.85", lw=0, label="noise band")
                ax.plot(times, diff, lw=0.8, color="tab:blue", label="A - B")
                ax.axhline(0.0, color="k", lw=0.5)
            else:
                shown = np.where(np.abs(diff) > thresholds, diff, np.nan) if mask_within_noise else diff
                mesh = ax.pcolormesh(
                    times, lat, shown.T, cmap="RdBu_r", vmin=-vmax[grid], vmax=vmax[grid], shading="nearest"
                )
                ax.set_ylabel("lat")
                if np.isnan(shown).all():
                    ax.text(0.5, 0.5, "no differences above noise", transform=ax.transAxes, ha="center", va="center")
                if i == len(scenarios) - 1:
                    fig.colorbar(mesh, ax=axes[:, j], label=f"A - B ({units})", shrink=0.6)

            if i == 0:
                ax.set_title(grid)
            if j == 0:
                ax.annotate(
                    scenario, (-0.3, 0.5), xycoords="axes fraction", rotation=90, va="center", fontweight="bold"
                )

    axes[0, 0].legend(loc="upper left", fontsize="small")
    fig.suptitle(f"{gas}: A - B ({units}), {frequency} files")

    return fig


def _esgf_records(source_id: str, search_url: str, data_node: str, page_size: int = 500) -> list[dict[str, Any]]:
    """Get the ESGF records of all latest, non-retracted files of a source ID on one data node"""
    session = requests.Session()
    retry = requests.adapters.Retry(
        total=10,
        backoff_factor=2,
        # The index rate-limits (429) and is sometimes overloaded (5xx)
        status_forcelist=(429, 500, 502, 503, 504),
        respect_retry_after_header=True,
    )
    session.mount("https://", requests.adapters.HTTPAdapter(max_retries=retry))
    records: list[dict[str, Any]] = []
    offset = 0
    while True:
        params = {
            "type": "File",
            "activity_id": "input4MIPs",
            "source_id": source_id,
            "data_node": data_node,
            "replica": "false",
            "latest": "true",
            "limit": page_size,
            "offset": offset,
        }
        # The index can be slow (minutes per query)
        response = session.get(search_url, params=params, timeout=600)
        response.raise_for_status()
        body = response.json()["response"]
        records.extend(body["docs"])
        offset += page_size
        if offset >= body["numFound"]:
            break

    # The index does not accept `retracted` as a filter, so drop retracted files here
    return [r for r in records if not r.get("retracted", False)]


def download_esgf_files(  # noqa: PLR0913
    source_ids: Iterable[str],
    out_bundle: Path,
    search_url: str = ESGF_SEARCH_URL,
    data_node: str = ESGF_DATA_NODE,
    n_threads: int = 8,
    refresh_file_list: bool = False,
) -> pd.DataFrame:
    """
    Download (mirror) ESGF files so they have the same layout as a run's output

    Files are saved under `out_bundle/data/processed/esgf-ready/input4MIPs/...`,
    following ESGF's own folder structure.
    Every file's sha256 checksum is checked against ESGF.
    Files that are already present with the right checksum are not downloaded again.

    Querying the ESGF index can take minutes,
    so the list of files found is saved in `out_bundle/esgf-file-list.json`
    and reused next time (use `refresh_file_list=True` to query again).

    Parameters
    ----------
    source_ids
        Source IDs to download (e.g. `["CR-vl-1-1-0", "CR-vl-ext-1-1-0"]`)

    out_bundle
        Bundle to download into

    search_url
        ESGF search index

    data_node
        ESGF data node to download from

    n_threads
        Number of downloads to run at the same time

    refresh_file_list
        Query the ESGF index again, even if a saved file list exists

    Returns
    -------
    :
        Number of files per source ID

    Raises
    ------
    LookupError
        A source ID has no files on ESGF, or a file has no HTTP download link
    """
    source_ids = list(source_ids)
    out_root = out_bundle / "data" / "processed" / "esgf-ready"
    file_list_path = out_bundle / ESGF_FILE_LIST_NAME

    file_list: dict[str, list[dict[str, Any]]] = {}
    if file_list_path.exists() and not refresh_file_list:
        file_list = json.loads(file_list_path.read_text())

    to_query = [s for s in source_ids if s not in file_list]
    if to_query:
        print(f"Querying ESGF for {len(to_query)} source IDs (this can take several minutes)")
        # More parallel queries than this get rate-limited by the index
        with ThreadPoolExecutor(max_workers=2) as executor:
            queried = executor.map(lambda s: _esgf_records(s, search_url=search_url, data_node=data_node), to_query)
            file_list.update(dict(zip(to_query, queried)))

        out_bundle.mkdir(parents=True, exist_ok=True)
        file_list_path.write_text(json.dumps(file_list, indent=1))

    to_get = []
    n_files = {}
    for source_id in source_ids:
        records = file_list[source_id]
        if not records:
            msg = f"No files found on ESGF for {source_id} ({search_url}, {data_node})"
            raise LookupError(msg)

        unique = {r["title"]: r for r in records}
        n_files[source_id] = len(unique)
        for title, record in unique.items():
            urls = [u.split("|")[0] for u in record["url"] if u.endswith("|HTTPServer")]
            if not urls:
                msg = f"No HTTP download link for {title}: {record['url']}"
                raise LookupError(msg)

            # publish_path looks like user_pub_work/input4MIPs/CMIP7/...
            rel_path = Path(record["publish_path"][0])
            rel_path = rel_path.relative_to(rel_path.parts[0])
            if record["checksum_type"][0].lower() != "sha256":
                msg = f"Unexpected checksum type for {title}: {record['checksum_type']}"
                raise NotImplementedError(msg)

            to_get.append((urls[0], f"sha256:{record['checksum'][0]}", out_root / rel_path))

    def retrieve(url: str, known_hash: str, dest: Path) -> None:
        pooch.retrieve(url, known_hash=known_hash, fname=dest.name, path=dest.parent, progressbar=False)

    pooch.get_logger().setLevel("WARNING")
    with ThreadPoolExecutor(max_workers=n_threads) as executor:
        futures = [executor.submit(retrieve, *args) for args in to_get]
        for future in tqdm.auto.tqdm(as_completed(futures), total=len(futures), desc="ESGF files"):
            future.result()

    return pd.Series(n_files, name="n_files").to_frame()
