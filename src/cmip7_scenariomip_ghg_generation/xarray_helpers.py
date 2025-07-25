"""
[xarray](https://github.com/pydata/xarray) helpers
"""

from __future__ import annotations

from functools import partial
from typing import TYPE_CHECKING, Protocol, overload

import cftime
import numpy as np

if TYPE_CHECKING:
    from collections.abc import Iterable
    from typing import Any, TypeVar

    import xarray as xr

    XRT = TypeVar("XRT", xr.DataArray, xr.Dataset)

MONTHS_PER_YEAR: int = 12
"""Months per year"""


class NonUniqueYearMonths(ValueError):
    """
    Raised when the user tries to convert to year-month with non-unique values

    This happens when the datetime values lead to year-month values that are
    not unique
    """

    def __init__(self, unique_vals: Iterable[tuple[int, int]], counts: Iterable[int]) -> None:
        """
        Initialise the error

        Parameters
        ----------
        unique_vals
            Unique values. In each tuple, the first value is the year and the
            second is the month.

        counts
            Counts of the number of time each unique value appeared in the
            original array
        """
        non_unique = list((v, c) for v, c in zip(unique_vals, counts) if c > 1)

        error_msg = "Your year-month axis is not unique. " f"Year-month values with a count > 1: {non_unique}"
        super().__init__(error_msg)


def convert_year_month_to_time(
    inp: XRT,
    day: int = 1,
    **kwargs: Any,
) -> XRT:
    """
    Convert year and month co-ordinates into a time axis

    This is a facade to :func:`convert_to_time`

    Parameters
    ----------
    inp
        Data to convert

    day
        Day of the month to assume in output

    **kwargs
        Passed to intialiser of :class:`cftime.datetime`
        If not supplied, we use a calendar of "standard".

    Returns
    -------
        Data with time axis
    """
    if not kwargs:
        kwargs = dict(calendar="standard")

    return convert_to_time(
        inp,
        time_coords=("year", "month"),
        cftime_converter=partial(cftime.datetime, day=day, **kwargs),
    )


def convert_year_to_time(
    inp: XRT,
    month: int = 6,
    day: int = 2,
    **kwargs: Any,
) -> XRT:
    """
    Convert year co-ordinates into a time axis

    This is a facade to :func:`convert_to_time`

    Parameters
    ----------
    inp
        Data to convert

    month
        Month to assume in output

    day
        Day of the month to assume in output

    **kwargs
        Passed to intialiser of :class:`cftime.datetime`.
        If not supplied, we use a calendar of "standard".

    Returns
    -------
        Data with time axis
    """
    if not kwargs:
        kwargs = dict(calendar="standard")

    return convert_to_time(
        inp,
        time_coords=("year",),
        cftime_converter=partial(cftime.datetime, month=month, day=day, **kwargs),
    )


class CftimeConverter(Protocol):  # pylint: disable=too-few-public-methods
    """
    Callable that supports converting stacked time co-ordinates to :obj:`cftime.datetime`
    """

    def __call__(
        self,
        *args: np.float64 | np.int64,
    ) -> cftime.datetime:
        """
        Convert input values to an :obj:`cftime.datetime`
        """


@overload
def convert_to_time(
    inp: xr.Dataset,
    time_coords: tuple[str, ...],
    cftime_converter: CftimeConverter,
) -> xr.Dataset: ...


@overload
def convert_to_time(
    inp: xr.DataArray,
    time_coords: tuple[str, ...],
    cftime_converter: CftimeConverter,
) -> xr.DataArray: ...


def convert_to_time(
    inp: xr.Dataset | xr.DataArray,
    time_coords: tuple[str, ...],
    cftime_converter: CftimeConverter,
) -> xr.Dataset | xr.DataArray:
    """
    Convert some co-ordinates representing time into a time axis

    Parameters
    ----------
    inp
        Data to convert

    time_coords
        Co-ordinates from which to create the time axis

    cftime_converter
        Callable that converts the stacked time co-ordinates to
        :obj:`cftime.datetime`

    Returns
    -------
        Data with time axis
    """
    inp = inp.stack(time=time_coords)
    times = inp["time"].to_numpy()

    inp = inp.drop_vars(("time", *time_coords)).assign_coords({"time": [cftime_converter(*t) for t in times]})

    return inp


@overload
def split_time_to_year_month(
    inp: xr.Dataset,
    time_axis: str = "time",
) -> xr.Dataset: ...


@overload
def split_time_to_year_month(
    inp: xr.DataArray,
    time_axis: str = "time",
) -> xr.DataArray: ...


def split_time_to_year_month(
    inp: xr.Dataset | xr.DataArray,
    time_axis: str = "time",
) -> xr.Dataset | xr.DataArray:
    """
    Convert the time dimension to year and month without stacking

    This means there is still a single time dimension in the output, but there
    is now also accompanying year and month information

    Parameters
    ----------
    inp
        Data to convert

    Returns
    -------
        Data with year and month information for the time axis

    Raises
    ------
    NonUniqueYearMonths
        The years and months are not unique
    """
    out = inp.assign_coords(
        {
            "month": inp[time_axis].dt.month,
            "year": inp[time_axis].dt.year,
        }
    ).set_index({time_axis: ("year", "month")})

    # Could be updated when https://github.com/pydata/xarray/issues/7104 is
    # closed
    unique_vals, counts = np.unique(  # type: ignore
        out[time_axis].values, return_counts=True
    )

    if (counts > 1).any():
        raise NonUniqueYearMonths(unique_vals, counts)

    return out


def convert_time_to_year_month(
    inp: XRT,
    time_axis: str = "time",
) -> XRT:
    """
    Convert the time dimension to year and month co-ordinates

    Parameters
    ----------
    inp
        Data to convert

    Returns
    -------
        Data with year and month co-ordinates
    """
    return split_time_to_year_month(
        inp=inp,
        time_axis=time_axis,
    ).unstack(time_axis)


def get_start_of_next_month(y: int, m: int) -> cftime.datetime:
    """
    Get start of next month

    Parameters
    ----------
    y
        Year

    m
        Month

    Returns
    -------
        Start of next month
    """
    if m == MONTHS_PER_YEAR:
        m_out = 1
        y_out = y + 1
    else:
        m_out = m + 1
        y_out = y

    # This may need to be refactored to allow the cftime_converter to be
    # injected, same idea as `convert_to_time`
    return cftime.datetime(y_out, m_out, 1)


def calculate_cos_lat_weighted_mean_latitude_only(
    inda: xr.DataArray,
    lat_name: str = "lat",
) -> xr.DataArray:
    """
    Calculate cos of latitude-weighted mean

    This is just a simple, cos of latitude-weighted mean of the input data.
    Implicitly, this assumes that the data only applies to the point it sits on,
    in contrast to {py:func}`calculate_area_weighted_mean_latitude_only`,
    which implicitly assumes that the data applies to the entire cell
    (and some other things,
    see the docstring of {py:func}`calculate_area_weighted_mean_latitude_only`).

    Parameters
    ----------
    inda
        Input data on which to calculate the mean

    lat_name
        Name of the latitudinal dimension in ``inda``

    Returns
    -------
    :
        Cos of latitude-weighted, latitudinal mean of ``inda``
    """
    weights = np.cos(np.deg2rad(inda[lat_name]))
    weights.name = "weights"

    return inda.weighted(weights=weights).mean(lat_name)


def calculate_global_mean_from_lon_mean(inda: xr.DataArray) -> xr.DataArray:
    """
    Calculate global-mean data from data which has already had a longitudinal mean applied.

    In other words, we assume that the data is on a latitudinal grid
    (with perhaps other non-spatial elements too).
    We also assume that the data applies to points, rather than areas.
    Hence we use {py:func}`calculate_cos_lat_weighted_mean_latitude_only`
    rather than {py:func}`calculate_area_weighted_mean_latitude_only`.

    Parameters
    ----------
    inda
        Input data

    Returns
    -------
    :
        Global-mean of ``inda``.
    """
    return calculate_cos_lat_weighted_mean_latitude_only(inda)


def calculate_area_weighted_mean_latitude_only(
    inp: xr.Dataset,
    variables: list[str],
    bounds_dim_name: str = "bounds",
    lat_name: str = "lat",
    lat_bounds_name: str = "lat_bounds",
) -> xr.Dataset:
    """
    Calculate an area-weighted mean based on only latitude information

    This assumes that the data applies to the entire cell
    and is constant across the cell,
    hence we're effectively doing a weighted integral
    of a piecewise-constant function,
    rather than a weighted sum
    (which is what pure cos-weighting implies).

    See :footcite:t:`kelly_savric_2020_computation`

    @article{kelly_savric_2020_computation,
        author = {Kelly, Kevin and Šavrič, Bojan},
        title = {
            Area and volume computation of longitude-latitude grids and three-dimensional meshes
        },
        journal = {Transactions in GIS},
        volume = {25},
        number = {1},
        pages = {6-24},
        doi = {https://doi.org/10.1111/tgis.12636},
        url = {https://onlinelibrary.wiley.com/doi/abs/10.1111/tgis.12636},
        eprint = {https://onlinelibrary.wiley.com/doi/pdf/10.1111/tgis.12636},
        abstract = {
            Abstract Longitude-latitude grids are commonly used for surface analyses
            and data storage in GIS. For volumetric analyses,
            three-dimensional meshes perpendicularly raised above or below the gridded surface are applied.
            Since grids and meshes are defined with geographic coordinates,
            they are not equal area or volume due to convergence of the meridians and radii.
            This article compiles and presents known geodetic considerations
            and relevant formulae needed for longitude-latitude grid and mesh analyses in GIS.
            The effect of neglecting these considerations is demonstrated on area
            and volume calculations of ecological marine units.
        },
        year = {2021}
    }

    Parameters
    ----------
    inp
        Data to process

    variables
        Variables of which to calculate the area-mean

    bounds_dim_name
        Name of the dimension which defines bounds

    lat_name
        Name of the latitude dimension

    lat_bounds_name
        Name of the latitude bounds variable

    Returns
    -------
    :
        Area-weighted mean of `variables`
    """
    lat_bnds = inp[lat_bounds_name].pint.to("radian")

    # The weights are effectively:
    # int_bl^bu r cos(theta) dphi r dtheta = int_bl^bu r^2 cos(theta) dtheta dphi
    # As they are constants, r^2 and longitude factors drop out in area weights.
    # (You would have to be more careful with longitudes if on a non-uniform grid).
    # When you evaluate the integral, you hence get that the weights are proportional to:
    # int_bl^bu cos(theta) dtheta = sin(bu) - sin(bl).
    # This is what we evaluate below.
    area_weighting = np.sin(lat_bnds).diff(dim=bounds_dim_name).squeeze()

    area_weighted_mean = (inp[variables] * area_weighting).sum(lat_name) / area_weighting.sum(lat_name)

    # May need to allow dependency injection in future here.
    keys_to_check = list(inp.data_vars.keys()) + list(inp.coords.keys())
    other_stuff = [v for v in keys_to_check if v not in variables]
    out = xr.merge([area_weighted_mean, inp[other_stuff]])

    return out
