"""
Mean-preserving interpolation of yearly values to monthly values
"""

from __future__ import annotations

from typing import Any

import cftime
import numpy as np
import numpy.typing as npt
import openscm_units
import pint
import xarray as xr

from cmip7_scenariomip_ghg_generation.mean_preserving_interpolation.core import (
    MeanPreservingInterpolationAlgorithmLike,
    mean_preserving_interpolation,
)
from cmip7_scenariomip_ghg_generation.mean_preserving_interpolation.lai_kaplan import (
    LaiKaplanInterpolator,
    get_wall_control_points_y_linear_with_flat_override_on_left,
)

N_MONTHS_PER_YEAR: int = 12
"""Number of months in a year"""

DEFAULT_ALGORITHM = LaiKaplanInterpolator(
    get_wall_control_points_y_from_interval_ys=get_wall_control_points_y_linear_with_flat_override_on_left,
    progress_bar=True,
    min_val=openscm_units.unit_registry.Quantity(0, "ppt"),
)


def interpolate_annual_mean_to_monthly(  # noqa: PLR0913
    values: npt.NDArray[float[Any]],
    values_units: str,
    years: npt.NDArray[int[Any]],
    years_units="yr",
    algorithm: MeanPreservingInterpolationAlgorithmLike = DEFAULT_ALGORITHM,
    month_rounding: int = 4,
    verify_output_is_mean_preserving: bool = True,
    unit_registry: pint.UnitRegistry = openscm_units.unit_registry,
    out_day: int = 15,
) -> xr.DataArray:
    """
    Interpolate annual-mean to monthly values, preserving the annual-mean

    Parameters
    ----------
    annual_mean
        Annual-mean value to interpolate

    algorithm
        Algorithm to use for the interpolation

    month_rounding
        Rounding to apply to the monthly float values.

        Unlikely that you'll need to change this.

    verify_output_is_mean_preserving
        Whether to verify that the output is mean-preserving before returning.

    out_day
        Day on which to place the output

    Returns
    -------
    :
        Monthly interpolated values
    """
    Q = unit_registry.Quantity
    y_in = Q(values, values_units)

    x_bounds_in = Q(np.hstack([years, years[-1] + 1.0]), years_units)
    x_bounds_out = Q(
        np.round(
            np.arange(x_bounds_in[0].m, x_bounds_in[-1].m + 1 / N_MONTHS_PER_YEAR / 2, 1 / N_MONTHS_PER_YEAR),
            month_rounding,
        ),
        "yr",
    )

    monthly_vals = mean_preserving_interpolation(
        x_bounds_in=x_bounds_in,
        y_in=y_in,
        x_bounds_out=x_bounds_out,
        algorithm=algorithm,
        verify_output_is_mean_preserving=verify_output_is_mean_preserving,
    )

    month_out = (x_bounds_out[1:] + x_bounds_out[:-1]) / 2.0

    time_out = [
        cftime.datetime(
            np.floor(time_val),
            np.round(N_MONTHS_PER_YEAR * (time_val % 1 + 1 / N_MONTHS_PER_YEAR / 2)),
            out_day,
        )
        for time_val in month_out.m
    ]

    out_time = xr.DataArray(
        data=monthly_vals,
        dims=["time"],
        coords=dict(time=time_out),
    )

    return out_time
