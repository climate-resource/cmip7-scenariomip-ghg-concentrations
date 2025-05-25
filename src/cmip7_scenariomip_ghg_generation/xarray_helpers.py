"""
xarray helpers
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
