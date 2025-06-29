"""
Lai-Kaplan mean-preserving interpolator

See [Lai and Kaplan, J. Atmos. Oceanic Technol. 2022](https://doi.org/10.1175/JTECH-D-21-0154.1)
"""

from __future__ import annotations

import warnings
from collections.abc import Callable
from functools import partial
from typing import Protocol, TypeVar, cast, overload

import attrs.validators
import numpy as np
import pint
import pint.testing
from attrs import define, field
from numpy.polynomial import Polynomial

from cmip7_scenariomip_ghg_generation.mean_preserving_interpolation.boundary_handling import (
    BoundaryHandling,
)
from cmip7_scenariomip_ghg_generation.mean_preserving_interpolation.grouping import get_group_indexes, get_group_sums
from cmip7_scenariomip_ghg_generation.mean_preserving_interpolation.rymes_meyers import RymesMeyersInterpolator

NPVal = TypeVar("NPVal", bound=np.generic)
# NPArray = TypeVar("NPArray", bound=npt.NDArray[Any])


@define
class LaiKaplanArray:
    """
    Thin wrapper around numpy arrays to support indexing like in the paper.

    This is sort of like writing a Python array that supports Fortran-style indexing,
    but trying to translate the paper with raw python indexes was too confusing,
    so we wrote this instead.
    """

    lai_kaplan_idx_min: float | int
    """Minimum index"""

    lai_kaplan_stride: float = field(validator=attrs.validators.in_((0.5, 1.0, 1)))
    """Size of stride"""

    data: pint.UnitRegistry.Quantity
    """Actual data array"""

    @property
    def max_allowed_lai_kaplan_index(self) -> float:
        """
        The maximum allowed Lai-Kaplan style index for `self.data`

        Returns
        -------
        :
            The index.
        """
        return cast(float, (self.data.size - 1) * self.lai_kaplan_stride + self.lai_kaplan_idx_min)

    @overload
    def to_data_index(self, idx_lai_kaplan: int | float, is_slice_idx: bool = False) -> int: ...

    @overload
    def to_data_index(self, idx_lai_kaplan: None, is_slice_idx: bool = False) -> None: ...

    def to_data_index(self, idx_lai_kaplan: int | float | None, is_slice_idx: bool = False) -> int | None:
        """
        Convert a Lai-Kaplan index to the equivalent index for `self.data`

        Parameters
        ----------
        idx_lai_kaplan
            Lai-Kaplan index to translate

        is_slice_idx
            Whether this index is a slice index.

            This is important to ensure we give sensible errors
            about whether the index is too big for `self.data` or not.

        Returns
        -------
        :
            The index for `self.data`.
        """
        if idx_lai_kaplan is None:
            return None

        if idx_lai_kaplan < self.lai_kaplan_idx_min:
            msg = f"{idx_lai_kaplan=} is less than {self.lai_kaplan_idx_min=}"
            raise IndexError(msg)

        idx_data_float = (idx_lai_kaplan - self.lai_kaplan_idx_min) / self.lai_kaplan_stride
        if idx_data_float % 1.0:
            msg = f"{idx_lai_kaplan=} leads to {idx_data_float=}, which is not an int. {self=}"
            raise IndexError(msg)

        idx_data = int(idx_data_float)

        if is_slice_idx:
            max_idx = self.data.size
        else:
            max_idx = self.data.size - 1

        if idx_data > max_idx:
            msg = (
                f"{idx_lai_kaplan=} leads to {idx_data=}, "
                f"which is outside the bounds of `self.data` ({self.data.size=}). "
                f"{self.max_allowed_lai_kaplan_index=}, {self=}"
            )
            raise IndexError(msg)

        return idx_data

    def to_data_step(self, step_lai_kaplan: int | float | None) -> int | None:
        """
        Translate a Lai-Kaplan step into the equivalent step for `self.data`

        Parameters
        ----------
        step_lai_kaplan
            Lai-Kaplan step size

        Returns
        -------
        :
            `self.data` step size
        """
        if step_lai_kaplan is None:
            return None

        step_data_float = step_lai_kaplan / self.lai_kaplan_stride
        if step_data_float % 1.0:
            msg = f"{step_lai_kaplan=} leads to {step_data_float=}, which is not an int. {self=}"
            raise IndexError(msg)

        step_data = int(step_data_float)

        return step_data

    def __getitem__(self, idx_lai_kaplan: int | float | slice) -> pint.UnitRegistry.Quantity:
        """
        Get an item from `self.data` using standard Python indexing

        The trick here is that we can use indexing like in the Lai-Kaplan paper
        and get the correct part of the underlying data array back.
        """
        if isinstance(idx_lai_kaplan, slice):
            idx_data: slice | int = slice(
                self.to_data_index(idx_lai_kaplan.start, is_slice_idx=True),
                self.to_data_index(idx_lai_kaplan.stop, is_slice_idx=True),
                self.to_data_step(idx_lai_kaplan.step),
            )

        else:
            idx_data = self.to_data_index(idx_lai_kaplan)

        return cast(pint.UnitRegistry.Quantity, self.data[idx_data])

    def __setitem__(self, idx_lai_kaplan: int | float | slice, val: pint.UnitRegistry.Quantity) -> None:
        """
        Set an item (or slice) in `self.data` using standard Python indexing

        The trick here is that we can use indexing like in the Lai-Kaplan paper
        and set the correct part of the underlying data array.
        """
        if isinstance(idx_lai_kaplan, slice):
            idx_data: slice | int = slice(
                self.to_data_index(idx_lai_kaplan.start, is_slice_idx=True),
                self.to_data_index(idx_lai_kaplan.stop, is_slice_idx=True),
                self.to_data_step(idx_lai_kaplan.step),
            )

        else:
            idx_data = self.to_data_index(idx_lai_kaplan)

        self.data[idx_data] = val


HERMITE_CUBICS: tuple[tuple[Polynomial, Polynomial], tuple[Polynomial, Polynomial]] = (
    (
        Polynomial((1, 0, -3, 2), domain=[0, 1], window=[0, 1]),
        Polynomial((0, 0, 3, -2), domain=[0, 1], window=[0, 1]),
    ),
    (
        Polynomial((0, 1, -2, 1), domain=[0, 1], window=[0, 1]),
        Polynomial((0, 0, -1, 1), domain=[0, 1], window=[0, 1]),
    ),
)
"""
Hermite cubic polynomials

Allows for the same notation as the paper.
"""

HERMITE_QUARTICS: tuple[tuple[Polynomial, Polynomial], tuple[Polynomial, Polynomial]] = (
    (cast(Polynomial, HERMITE_CUBICS[0][0].integ()), cast(Polynomial, HERMITE_CUBICS[0][1].integ())),  # type: ignore[no-untyped-call]
    (cast(Polynomial, HERMITE_CUBICS[1][0].integ()), cast(Polynomial, HERMITE_CUBICS[1][1].integ())),  # type: ignore[no-untyped-call]
)
"""
Hermite quartic polynomials

Allows for the same notation as the paper.
"""


@define
class LaiKaplanF:
    """
    Lai-Kaplan interpolating function
    """

    x_i: pint.UnitRegistry.Quantity
    """Start of the interval over which this function applies"""

    delta: pint.UnitRegistry.Quantity
    """Size of the domain over which this function applies"""

    s_i: pint.UnitRegistry.Quantity
    """Value at the left-hand edge of the domain (`x = x_i`)"""

    s_i_plus_half: pint.UnitRegistry.Quantity
    """Value at the right-hand edge of the domain (`x = x_i + delta`)"""

    m_i: pint.UnitRegistry.Quantity
    """Gradient at the left-hand edge of the domain (`x = x_i`)"""

    m_i_plus_half: pint.UnitRegistry.Quantity
    """Gradient at the right-hand edge of the domain (`x = x_i + delta`)"""

    def calculate(
        self,
        x: pint.UnitRegistry.Quantity,
        check_domain: bool = True,
    ) -> pint.UnitRegistry.Quantity:
        """
        Calculate Lai-Kaplan interpolating function value

        Parameters
        ----------
        x
            Value for which we want to calculate the value of the function

        check_domain
            Whether to check that `x` is in the supported domain before calculating.

        Returns
        -------
        :
            Function value at `x`, given the other parameters
        """
        if check_domain:
            if (x < self.x_i) or (x > self.x_i + self.delta):
                msg = f"x is outside the supported domain. {x=} {self.x_i=} {self.x_i + self.delta=}"
                raise ValueError(msg)

        u = (x - self.x_i) / self.delta

        return self.calculate_u(u, check_domain=False)

    def calculate_integral_indefinite(
        self,
        x: pint.UnitRegistry.Quantity,
        check_domain: bool = True,
    ) -> pint.UnitRegistry.Quantity:
        """
        Calculate the indefinite integral of the Lai-Kaplan interpolating function value

        This is just the indefinite integral, i.e. is missing an integrating constant.
        For integration, see [`calculate_integral_definite`][].

        Parameters
        ----------
        x
            x-value for which we want to calculate the indefinite integral.

        check_domain
            Whether to check that `x` is in the supported domain before calculating.

        Returns
        -------
        :
            Indefinite integral of the Lai-Kaplan interpolating function.
        """
        if check_domain:
            if (x < self.x_i) or (x > self.x_i + self.delta):
                msg = "`x` is outside the supported domain. " f"{x=} {self.x_i=} {self.x_i + self.delta=}"
                raise ValueError(msg)

        u = (x - self.x_i) / self.delta

        res = cast(
            pint.UnitRegistry.Quantity,
            self.delta
            * (
                self.s_i * HERMITE_QUARTICS[0][0](u)
                + self.delta * self.m_i * HERMITE_QUARTICS[1][0](u)
                + self.s_i_plus_half * HERMITE_QUARTICS[0][1](u)
                + self.delta * self.m_i_plus_half * HERMITE_QUARTICS[1][1](u)
            ),
        )

        return res

    def calculate_integral_indefinite_unitless(
        self,
        x: float,
        check_domain: bool = True,
    ) -> float:
        """
        Calculate the indefinite integral of the Lai-Kaplan interpolating function value

        The calculation is performed without considering units.

        This is just the indefinite integral, i.e. is missing an integrating constant.
        For integration, see [`calculate_integral_definite`][].

        Parameters
        ----------
        x
            x-value for which we want to calculate the indefinite integral.

        check_domain
            Whether to check that `x` is in the supported domain before calculating.

        Returns
        -------
        :
            Indefinite integral of the Lai-Kaplan interpolating function.
        """
        if check_domain:
            if (x < self.x_i.m) or (x > self.x_i.m + self.delta.m):
                msg = "`x` is outside the supported domain. " f"{x=} {self.x_i=} {self.x_i + self.delta=}"
                raise ValueError(msg)

        u = (x - self.x_i.m) / self.delta.m

        res = cast(
            float,
            self.delta.m
            * (
                self.s_i.m * HERMITE_QUARTICS[0][0](u)
                + self.delta.m * self.m_i.m * HERMITE_QUARTICS[1][0](u)
                + self.s_i_plus_half.m * HERMITE_QUARTICS[0][1](u)
                + self.delta.m * self.m_i_plus_half.m * HERMITE_QUARTICS[1][1](u)
            ),
        )

        return res

    def calculate_integral_definite(
        self,
        x_lower: pint.UnitRegistry.Quantity,
        x_upper: pint.UnitRegistry.Quantity,
        check_domain: bool = True,
    ) -> pint.UnitRegistry.Quantity:
        """
        Calculate the definite integral of the Lai-Kaplan interpolating function value

        Parameters
        ----------
        x_lower
            Lower x-bound for the domain over which we want to calculate the integral.

        x_upper
            Upper x-bound for the domain over which we want to calculate the integral.

        check_domain
            Whether to check that `x_lower` and `x_upper` is in the supported domain before calculating.

        Returns
        -------
        :
            Integral of the Lai-Kaplan interpolating function from `x_lower` to `x_upper`.
        """
        if x_lower >= x_upper:
            msg = f"`x_lower` must be less than `x_upper`. {x_lower=} {x_upper=}"
            raise ValueError(msg)

        res = cast(
            pint.UnitRegistry.Quantity,
            self.calculate_integral_indefinite(x_upper, check_domain=check_domain)
            - self.calculate_integral_indefinite(x_lower, check_domain=check_domain),
        )

        return res

    def calculate_integral_definite_unitless(
        self,
        x_lower: float,
        x_upper: float,
        check_domain: bool = True,
    ) -> float:
        """
        Calculate the definite integral of the Lai-Kaplan interpolating function value

        The calculation is performed without considering units.

        Parameters
        ----------
        x_lower
            Lower x-bound for the domain over which we want to calculate the integral.

        x_upper
            Upper x-bound for the domain over which we want to calculate the integral.

        check_domain
            Whether to check that `x_lower` and `x_upper` is in the supported domain before calculating.

        Returns
        -------
        :
            Integral of the Lai-Kaplan interpolating function from `x_lower` to `x_upper`.
        """
        if x_lower >= x_upper:
            msg = f"`x_lower` must be less than `x_upper`. {x_lower=} {x_upper=}"
            raise ValueError(msg)

        res = self.calculate_integral_indefinite_unitless(
            x_upper, check_domain=check_domain
        ) - self.calculate_integral_indefinite_unitless(x_lower, check_domain=check_domain)

        return res

    def calculate_unitless(
        self,
        x: float,
        check_domain: bool = True,
    ) -> float:
        """
        Calculate Lai-Kaplan interpolating function value

        Do the calculation without units.
        This is helpful for integrating the function with scipy.

        Parameters
        ----------
        x
            Value for which we want to calculate the value of the function

        check_domain
            Whether to check that `x` is in the supported domain before calculating.

        Returns
        -------
        :
            Function value at `x`, given the other parameters
        """
        if check_domain:
            if (x < self.x_i.m) or (x > self.x_i.m + self.delta.m):
                msg = f"x is outside the supported domain. {x=} {self.x_i=} {self.x_i + self.delta=}"
                raise ValueError(msg)

        u = (x - self.x_i.m) / self.delta.m

        res = cast(
            float,
            (
                self.s_i.m * HERMITE_CUBICS[0][0](u)
                + self.delta.m * self.m_i.m * HERMITE_CUBICS[1][0](u)
                + self.s_i_plus_half.m * HERMITE_CUBICS[0][1](u)
                + self.delta.m * self.m_i_plus_half.m * HERMITE_CUBICS[1][1](u)
            ),
        )

        return res

    def calculate_u(
        self,
        u: float | pint.UnitRegistry.Quantity,
        check_domain: bool = True,
    ) -> pint.UnitRegistry.Quantity:
        """
        Calculate Lai-Kaplan interpolating function value

        Parameters
        ----------
        u
            Value for which we want to calculate the value of the function.

            This should have been normalised first i.e. this is in 'u-space', not 'x-space'.

        check_domain
            Whether to check that `u` is in the supported domain before calculating.

        Returns
        -------
        :
            Function value at `u`, given the other parameters
        """
        if check_domain:
            if (u < 0) or (u > 1):
                msg = f"u is outside the supported domain. {u=}"
                raise ValueError(msg)

        res = cast(
            pint.UnitRegistry.Quantity,
            (
                self.s_i * HERMITE_CUBICS[0][0](u)
                + self.delta * self.m_i * HERMITE_CUBICS[1][0](u)
                + self.s_i_plus_half * HERMITE_CUBICS[0][1](u)
                + self.delta * self.m_i_plus_half * HERMITE_CUBICS[1][1](u)
            ),
        )

        return res


class ExtrapolateYIntervalValuesLike(Protocol):
    """
    Class that can be used for extrapolating the y-values for the external intervals
    """

    def __call__(
        self,
        x_in: pint.UnitRegistry.Quantity,
        y_in: pint.UnitRegistry.Quantity,
        x_out: pint.UnitRegistry.Quantity,
    ) -> pint.UnitRegistry.Quantity:
        """
        Extrapolate our y-interval values to get an extra value either side of the input domain

        Parameters
        ----------
        x_in
            x-values of the input array

        y_in
            y-values of the input array

        x_out
            x-values to extrapolate

            There should be two: the x-value to the left of `x_in`
            and the x-value to the right of `x_in`.

        Returns
        -------
        :
            The extrapolated values at `x_out`.
        """


def extrapolate_y_interval_values(
    x_in: pint.UnitRegistry.Quantity,
    y_in: pint.UnitRegistry.Quantity,
    x_out: pint.UnitRegistry.Quantity,
    left: BoundaryHandling = BoundaryHandling.CONSTANT,
    right: BoundaryHandling = BoundaryHandling.CUBIC_EXTRAPOLATION,
) -> pint.UnitRegistry.Quantity:
    """
    Extrapolate our y-interval values to get an extra value either side of the input domain

    Parameters
    ----------
    x_in
        x-values of the input array

    y_in
        y-values of the input array

    x_out
        x-values to extrapolate

        There should be two: the x-value to the left of `x_in`
        and the x-value to the right of `x_in`.

    left
        The extrapolation method to use for the left-hand value.

    right
        The extrapolation method to use for the right-hand value.

    Returns
    -------
    :
        The extrapolated values at `x_out`.
    """
    expected_out_size = 2

    if x_out.size != expected_out_size:
        raise NotImplementedError

    y_out = np.nan * np.zeros(expected_out_size) * y_in.u

    if any(bh == BoundaryHandling.CUBIC_EXTRAPOLATION for bh in (left, right)):
        # # TODO: switch to optional pattern
        # scipy_inter = get_optional_dependency("scipy.interpolate")
        import scipy.interpolate as scipy_inter

        cubic_interpolator = scipy_inter.interp1d(
            x_in.m,
            y_in.m,
            kind="cubic",
            fill_value="extrapolate",
        )

    if left == BoundaryHandling.CONSTANT:
        y_out[0] = y_in[0]
    elif left == BoundaryHandling.CUBIC_EXTRAPOLATION:
        y_out[0] = cubic_interpolator(x_out[0].m) * y_in.u
    else:
        raise NotImplementedError(left)

    if right == BoundaryHandling.CUBIC_EXTRAPOLATION:
        y_out[-1] = cubic_interpolator(x_out[-1].m) * y_in.u
    elif right == BoundaryHandling.CONSTANT:
        y_out[-1] = y_in[-1]
    else:
        raise NotImplementedError(right)

    return cast(pint.UnitRegistry.Quantity, np.hstack(y_out))


class MinValApplierLike(Protocol):
    """
    Class that can be used for ensuring the solution obeys the minimum value criteria
    """

    def iterate_to_solution(  # noqa: PLR0913
        self,
        starting_values: pint.UnitRegistry.Quantity,
        x_bounds_out: pint.UnitRegistry.Quantity,
        x_bounds_in: pint.UnitRegistry.Quantity,
        y_in: pint.UnitRegistry.Quantity,
        left_bound_val: pint.UnitRegistry.Quantity,
        right_bound_val: pint.UnitRegistry.Quantity,
        min_val: pint.UnitRegistry.Quantity,
    ) -> pint.UnitRegistry.Quantity:
        """
        Iterate to the solution

        Parameters
        ----------
        starting_values
            Starting values for the iterations

        x_bounds_out
            x-bounds to which we want to interpolate

        x_bounds_in
            x-bounds of the input values

        y_in
            y-values of the input values

        left_bound_val
            Value to use for the left boundary of the domain while iterating

        right_bound_val
            Value to use for the right boundary of the domain while iterating

        min_val
            Minimum value allowed in the solution

        Returns
        -------
        :
            Solution (i.e. the result of the iterations)
        """


def get_min_val_applier_default(lai_kaplan_interpolator: LaiKaplanInterpolator) -> RymesMeyersInterpolator:
    """
    Get minimum value applier

    In other words, get the class we can use to ensure that our solutions
    obey any minimum value criteria.

    This is the default implementation.
    Others can be used to inject different behaviour.

    Parameters
    ----------
    lai_kaplan_interpolator
        The Lai-Kaplan interpolator, whose solution we want to apply the minimum value to.

    Returns
    -------
    :
        Class which can be used to updated the solution to obey the minimum value.
    """
    rm_interpolator = RymesMeyersInterpolator(
        min_it=1,
        min_val=lai_kaplan_interpolator.min_val,
        atol=lai_kaplan_interpolator.atol,
        rtol=lai_kaplan_interpolator.rtol,
        progress_bar=lai_kaplan_interpolator.progress_bar,
    )

    return rm_interpolator


def get_wall_control_points_y_linear(
    intervals_x: pint.UnitRegistry.Quantity,
    intervals_y: pint.UnitRegistry.Quantity,
    control_points_wall_x: pint.UnitRegistry.Quantity,
) -> pint.UnitRegistry.Quantity:
    """
    Get y-values at wall control points using linear interpolation

    Parameters
    ----------
    intervals_x
        The x-values at the mid-point of each interval

    intervals_y
        The y-values for each interval.

        These y-values are the average value over each interval.

    control_points_wall_x
        The x-values at each wall control point

    Returns
    -------
    :
        y-values at each wall control point.
    """
    control_points_wall_y = cast(pint.UnitRegistry.Quantity, np.interp(control_points_wall_x, intervals_x, intervals_y))

    return control_points_wall_y


def get_wall_control_points_y_linear_with_flat_override_on_left(
    intervals_x: pint.UnitRegistry.Quantity,
    intervals_y: pint.UnitRegistry.Quantity,
    control_points_wall_x: pint.UnitRegistry.Quantity,
) -> pint.UnitRegistry.Quantity:
    """
    Get y-values at wall control points using linear interpolation and keeping initially flat values flat.

    If the values start out flat,
    we keep them flat right up to the end of the last interval
    that has the same value as the first value.
    This can help to avoid values increasing before one might intuitively expect they should.

    Parameters
    ----------
    intervals_x
        The x-values at the mid-point of each interval

    intervals_y
        The y-values for each interval.

        These y-values are the average value over each interval.

    control_points_wall_x
        The x-values at each wall control point

    Returns
    -------
    :
        y-values at each wall control point.
    """
    control_points_wall_y = get_wall_control_points_y_linear(
        intervals_x=intervals_x,
        intervals_y=intervals_y,
        control_points_wall_x=control_points_wall_x,
    )

    # If the values start out flat, keep them flat right until the end of the flat intervals.
    first_change = np.argmax(np.abs(np.diff(intervals_y)) > 0)
    if first_change > 0:
        control_points_wall_y[first_change] = intervals_y[0]

    return control_points_wall_y


def get_wall_control_points_y_cubic(
    intervals_x: pint.UnitRegistry.Quantity,
    intervals_y: pint.UnitRegistry.Quantity,
    control_points_wall_x: pint.UnitRegistry.Quantity,
) -> pint.UnitRegistry.Quantity:
    """
    Get y-values at wall control points using a cubic spline

    Parameters
    ----------
    intervals_x
        The x-values at the mid-point of each interval

    intervals_y
        The y-values for each interval.

        These y-values are the average value over each interval.

    control_points_wall_x
        The x-values at each wall control point

    Returns
    -------
    :
        y-values at each wall control point.
    """
    # # TODO: switch to optional pattern
    # scipy_inter = get_optional_dependency("scipy.interpolate")
    import scipy.interpolate as scipy_interp

    cubic_interpolator = scipy_interp.interp1d(
        intervals_x.m,
        intervals_y.m,
        kind="cubic",
        fill_value="extrapolate",
    )
    control_points_wall_y = cast(
        pint.UnitRegistry.Quantity, cubic_interpolator(control_points_wall_x.m) * intervals_y.u
    )

    return control_points_wall_y


class GetWallControlPointsY(Protocol):
    """
    Callable that can be used to get the y-values at the wall control points
    """

    def __call__(
        self,
        intervals_x: pint.UnitRegistry.Quantity,
        intervals_y: pint.UnitRegistry.Quantity,
        control_points_wall_x: pint.UnitRegistry.Quantity,
    ) -> pint.UnitRegistry.Quantity:
        """
        Get y-values at wall control points

        Parameters
        ----------
        intervals_x
            The x-values at the mid-point of each interval

        intervals_y
            The y-values for each interval.

            These y-values are the average value over each interval.

        control_points_wall_x
            The x-values at each wall control point

        Returns
        -------
        :
            y-values at each wall control point.
        """


@define
class LaiKaplanInterpolator:
    """
    Lai-Kaplan mean-preserving interpolator

    This splits each interval in half,
    then fits a mean-preserving cubic spline across each interval.
    The use of cubic splines means things can go a bit weird, but it is extremely fast.
    The option to specify minimum and maximum bounds for values
    allows you to limit some of the more extreme excursions.
    However, this boundary application is done using a Rymes-Meyers
    (see [`rymes_meyers`][cmip7_scenariomip_ghg_generation.mean_preserving_interpolation.rymes_meyers])
    style algorithm, so is much slower.

    See [Lai and Kaplan, J. Atm. Ocn. Tech. 2022](https://doi.org/10.1175/JTECH-D-21-0154.1)
    """

    extrapolate_y_interval_values: ExtrapolateYIntervalValuesLike = partial(
        extrapolate_y_interval_values,
        left=BoundaryHandling.CONSTANT,
        right=BoundaryHandling.CUBIC_EXTRAPOLATION,
    )
    """
    Function that calculates the extrapolated y interval values from the input data

    This function is given the input y-values, plus the mid-point of each interval.
    """

    get_wall_control_points_y_from_interval_ys: GetWallControlPointsY = get_wall_control_points_y_cubic
    """
    Function that calculates the y-values at the wall control points from the averages over each interval
    """

    get_min_val_applier: Callable[[LaiKaplanInterpolator], MinValApplierLike] = get_min_val_applier_default
    """
    Rymes-Meyers interpolator

    Used to create a new solution when values in the initial solution
    are less than `self.min_val`.
    """

    min_val: pint.UnitRegistry.Quantity | None = None
    """
    Minimum value that can appear in the solution
    """

    atol: float = 1e-10
    """
    Absolute tolerance for deciding whether the output value means are close to the input means
    """

    rtol: float = 1e-7
    """
    Relative tolerance for deciding whether the output value means are close to the input means
    """

    rtol_uniform_steps: float = 1e-7
    """
    Relative tolerance for deciding whether the input steps are uniform
    """

    progress_bar: bool = True
    """
    Whether to show a progress bar while filling the output array or not
    """

    def __call__(
        self,
        x_bounds_in: pint.UnitRegistry.Quantity,
        y_in: pint.UnitRegistry.Quantity,
        x_bounds_out: pint.UnitRegistry.Quantity,
    ) -> pint.UnitRegistry.Quantity:
        """
        Perform mean-preserving interpolation

        Parameters
        ----------
        x_bounds_in
            Bounds of the x-range to which each value in `y_in` applies.

        y_in
            y-values for each interval in `x_bounds_in`.

        x_bounds_out
            Bounds of the x-values onto which to interpolate `y_in`.

        Returns
        -------
        :
            Interpolated, mean-preserving values
        """
        if issubclass(y_in.m.dtype.type, np.integer):
            msg = "The input will be converted to a floating type. " "If we don't do this, the algorithm doesn't work."
            warnings.warn(msg)
            y_in = y_in * 1.0  # make sure that y_in is float type

        if not np.all(x_bounds_out[:-1] <= x_bounds_out[1:]):
            msg = f"x_bounds_out must be sorted for this to work {x_bounds_out=}"
            raise AssertionError(msg)

        x_bounds_out = cast(pint.UnitRegistry.Quantity, x_bounds_out.to(x_bounds_in.u))

        x_steps = x_bounds_in[1:] - x_bounds_in[:-1]
        x_step = x_steps[0]
        pint.testing.assert_allclose(
            x_step, x_steps, rtol=self.rtol_uniform_steps, msg="Non-uniform spacing in x_bounds_in"
        )

        delta = x_step / 2.0
        intervals_internal_x = (x_bounds_in[1:] + x_bounds_in[:-1]) / 2.0
        walls_x = x_bounds_in
        intervals_x = cast(
            pint.UnitRegistry.Quantity,
            np.hstack(
                [
                    intervals_internal_x[0] - x_step,
                    intervals_internal_x,
                    intervals_internal_x[-1] + x_step,
                ]
            ),
        )

        n_lai_kaplan = y_in.size

        control_points_x_d = (
            np.zeros(
                2 * x_bounds_in.size + 1,
                # Has to be float so we can handle half steps even if input x array is integer
                dtype=np.float64,
            )
            * x_bounds_in.u
        )
        # Control points on the walls
        control_points_x_d[1::2] = walls_x
        # Control points in the intervals
        control_points_x_d[::2] = intervals_x

        control_points_x = LaiKaplanArray(
            lai_kaplan_idx_min=1 / 2,
            lai_kaplan_stride=1 / 2,
            data=control_points_x_d,
        )

        external_intervals_y_d = self.extrapolate_y_interval_values(
            x_in=intervals_internal_x,
            y_in=y_in,
            x_out=cast(pint.UnitRegistry.Quantity, np.hstack([intervals_x[0], intervals_x[-1]])),
        )
        intervals_y = LaiKaplanArray(
            lai_kaplan_idx_min=0.0,
            lai_kaplan_stride=1.0,
            data=cast(
                pint.UnitRegistry.Quantity,
                np.hstack([external_intervals_y_d[0], y_in, external_intervals_y_d[-1]]),
            ),
        )

        control_points_wall_y_d = self.get_wall_control_points_y_from_interval_ys(
            intervals_x=intervals_x,
            intervals_y=intervals_y.data,
            control_points_wall_x=x_bounds_in,
        )
        control_points_wall_y = LaiKaplanArray(
            lai_kaplan_idx_min=1,
            lai_kaplan_stride=1,
            data=control_points_wall_y_d,
        )

        control_points_y = self.solve_control_points_y(
            n_lai_kaplan=n_lai_kaplan,
            x_step=x_step,
            target=y_in,
            control_points_x=control_points_x,
            control_points_wall_y=control_points_wall_y,
            external_control_points_y_d=external_intervals_y_d,
        )

        y_out = self.create_y_out_from_control_points(
            target=y_in,
            x_bounds_out=x_bounds_out,
            control_points_x=control_points_x,
            control_points_y=control_points_y,
            n_lai_kaplan=n_lai_kaplan,
            delta=delta,
        )

        if self.min_val is not None and (y_out < self.min_val).any():
            if (y_in < self.min_val).any():
                msg = (
                    "There are values in `y_in` that are less than the minimum value. "
                    "This isn't going to work. "
                    f"{y_in.min()=} {self.min_val=}"
                )
                raise AssertionError(msg)

            y_out = self.lower_bound_adjustment(
                y_out=y_out,
                control_points_y=control_points_y,
                x_bounds_target=x_bounds_in,
                target=y_in,
                x_bounds_out=x_bounds_out,
            )

        return y_out

    def solve_control_points_y(  # noqa: PLR0913
        self,
        n_lai_kaplan: int,
        x_step: pint.UnitRegistry.Quantity,
        target: pint.UnitRegistry.Quantity,
        control_points_x: LaiKaplanArray,
        control_points_wall_y: LaiKaplanArray,
        external_control_points_y_d: pint.UnitRegistry.Quantity,
    ) -> LaiKaplanArray:
        """
        Solve for the y-values at the control points

        Parameters
        ----------
        n_lai_kaplan
            The Lai-Kaplan "n" value

        x_step
            The size of the x-step.

            `delta` is calculated internally and is assumed to be half of `x_step`.

        target
            The mean in each interval we want our solution to match.

        control_points_x
            The x-values of the control points.

        control_points_wall_y
            The y-values of the control points at the walls (i.e. the boundary of each interval).

        external_control_points_y_d
            The y-values of the control points to be used for the external control points.

        Returns
        -------
        :
            The solved y-values of the control points.
        """
        delta = x_step / 2.0

        a_d = np.array(
            [
                -HERMITE_QUARTICS[1][0](1) / 2.0,
                (
                    HERMITE_QUARTICS[0][0](1)
                    + HERMITE_QUARTICS[0][1](1)
                    + HERMITE_QUARTICS[1][0](1) / 2.0
                    - HERMITE_QUARTICS[1][1](1) / 2.0
                ),
                HERMITE_QUARTICS[1][1](1) / 2.0,
            ]
        )
        a = LaiKaplanArray(
            lai_kaplan_idx_min=1,
            lai_kaplan_stride=1,
            data=a_d,  # type: ignore # given up on making this nicer
        )

        # A-matrix
        # (Not indexed in the paper, hence not done with Lai Kaplan indexing)
        A_mat = np.zeros((n_lai_kaplan, n_lai_kaplan))
        rows, cols = np.diag_indices_from(A_mat)
        A_mat[rows[1:], cols[:-1]] = a[1]
        A_mat[rows, cols] = a[2]
        A_mat[rows[:-1], cols[1:]] = a[3]

        # Area under the curve in each interval
        A_d = x_step * target
        A = LaiKaplanArray(lai_kaplan_idx_min=1, lai_kaplan_stride=1, data=A_d)

        # beta array
        beta_d = np.array(
            [
                (HERMITE_QUARTICS[0][0](1) - HERMITE_QUARTICS[1][0](1) / 2.0 - HERMITE_QUARTICS[1][1](1) / 2.0),
                (HERMITE_QUARTICS[0][1](1) + HERMITE_QUARTICS[1][0](1) / 2.0 + HERMITE_QUARTICS[1][1](1) / 2.0),
            ]
        )
        beta = LaiKaplanArray(1, 1, beta_d)  # type: ignore # given up making this nicer

        b = LaiKaplanArray(
            lai_kaplan_idx_min=1,
            lai_kaplan_stride=1,
            data=np.zeros_like(target.data) * target.u,
        )
        b[1] = (
            A[1] / delta
            - beta[1] * control_points_wall_y[1]
            - beta[2] * control_points_wall_y[2]
            - a[1] * external_control_points_y_d[0]
        )
        middle_slice = slice(2, n_lai_kaplan)
        middle_slice_plus_one = slice(3, n_lai_kaplan + 1)
        b[middle_slice] = (
            A[middle_slice] / delta
            - beta[1] * control_points_wall_y[middle_slice]
            - beta[2] * control_points_wall_y[middle_slice_plus_one]
        )
        b[n_lai_kaplan] = (
            A[n_lai_kaplan] / delta
            - beta[1] * control_points_wall_y[n_lai_kaplan]
            - beta[2] * control_points_wall_y[n_lai_kaplan + 1]
            - a[3] * external_control_points_y_d[-1]
        )

        control_points_interval_y_d = cast(pint.UnitRegistry.Quantity, np.linalg.solve(A_mat, b.data))
        # # Not needed, but helpful double check if debugging
        # pint.testing.assert_allclose(
        #     np.dot(A_mat, control_points_interval_y_d.m), b.data.m, atol=self.atol, rtol=self.rtol
        # )

        control_points_y = LaiKaplanArray(
            lai_kaplan_idx_min=1 / 2,
            lai_kaplan_stride=1 / 2,
            data=cast(
                pint.UnitRegistry.Quantity,
                np.nan * np.zeros_like(control_points_x.data) * control_points_interval_y_d.u,
            ),
        )
        # Pre-calculated external interval values
        control_points_y[1 / 2] = external_control_points_y_d[0]
        control_points_y[n_lai_kaplan + 3 / 2] = external_control_points_y_d[-1]
        control_points_y[1 : n_lai_kaplan + 1 + 1 : 1] = control_points_wall_y[:]
        # Calculated values
        control_points_y[3 / 2 : n_lai_kaplan + 1 / 2 + 1 : 1] = control_points_interval_y_d  # type: ignore # mypy confused by hacky slicing

        return control_points_y

    def create_y_out_from_control_points(  # noqa: PLR0913
        self,
        target: pint.UnitRegistry.Quantity,
        x_bounds_out: pint.UnitRegistry.Quantity,
        control_points_x: LaiKaplanArray,
        control_points_y: LaiKaplanArray,
        n_lai_kaplan: int,
        delta: pint.UnitRegistry.Quantity,
    ) -> pint.UnitRegistry.Quantity:
        """
        Create our solution for `y_out` based on our solved control points

        Parameters
        ----------
        target
            The mean in each interval we want our solution to match.

        x_bounds_out
            The bounds of each interval in our output solution.

        control_points_x
            The x-values of our control points

        control_points_y
            The y-values of our control points

        n_lai_kaplan
            The Lai-Kaplan "n" value

        delta
            The size of each window in our Lai-Kaplan solution space

        Returns
        -------
        :
            y-values that correspond to our solved control points.
        """
        gradients_at_control_points = LaiKaplanArray(
            lai_kaplan_idx_min=1,
            lai_kaplan_stride=1 / 2,
            data=np.nan * np.zeros(2 * n_lai_kaplan + 1) * (control_points_y.data.u / delta.u),
        )
        gradients_at_control_points[:] = (
            control_points_y[3 / 2 : n_lai_kaplan + 1 + 1] - control_points_y[1 / 2 : n_lai_kaplan + 1]  # type: ignore # mypy confused by hacky slicing
        ) / (2 * delta)

        # TODO: Can't see how to calculate the result with vectors,
        # maybe someone else can.
        y_out_m = np.nan * np.zeros(x_bounds_out.size - 1)
        iterh = range(y_out_m.size)
        if self.progress_bar:
            # # TODO: switch to optional pattern
            # tqdman = get_optional_dependency("tqdm.auto")
            import tqdm.auto as tqdman

            iterh = tqdman.tqdm(iterh, desc="Calculating output values")

        lai_kaplan_interval_idx = 1 / 2
        x_i = control_points_x[lai_kaplan_interval_idx] - 10 * delta
        for out_index in iterh:
            if x_bounds_out[out_index] >= x_i + delta:
                lai_kaplan_interval_idx += 1 / 2

                x_i = control_points_x[lai_kaplan_interval_idx]
                lai_kaplan_f = LaiKaplanF(
                    x_i=x_i,
                    delta=delta,
                    s_i=control_points_y[lai_kaplan_interval_idx],
                    s_i_plus_half=control_points_y[lai_kaplan_interval_idx + 1 / 2],
                    m_i=gradients_at_control_points[lai_kaplan_interval_idx],
                    m_i_plus_half=gradients_at_control_points[lai_kaplan_interval_idx + 1 / 2],
                )

            integral_m = lai_kaplan_f.calculate_integral_definite_unitless(
                x_bounds_out[out_index].m, x_bounds_out[out_index + 1].m
            )
            average_m = integral_m / (x_bounds_out[out_index + 1].m - x_bounds_out[out_index].m)
            y_out_m[out_index] = average_m

        y_out = cast(pint.UnitRegistry.Quantity, y_out_m * target.u)

        return y_out

    def lower_bound_adjustment(
        self,
        y_out: pint.UnitRegistry.Quantity,
        control_points_y: LaiKaplanArray,
        x_bounds_target: pint.UnitRegistry.Quantity,
        target: pint.UnitRegistry.Quantity,
        x_bounds_out: pint.UnitRegistry.Quantity,
    ) -> pint.UnitRegistry.Quantity:
        """
        Adjust the solution to account for the fact that some values are below a specified minimum

        Parameters
        ----------
        y_out
            The current solution.

            This is modified in-place.

        control_points_y
            The y-value at each control point.

        x_bounds_target
            The x-bounds of the target

        target
            The target values

        x_bounds_out
            The x-bounds onto which we are interpolating

        Returns
        -------
        :
            The updated solution values based on adjusting for the lower bound.
        """
        if self.min_val is None:
            msg = "`self.min_val` should not be `None` if you're calling this method"
            raise AssertionError(msg)

        # Apply some sense
        min_val_same_u = self.min_val.to(y_out.u)
        already_close_to_min_val = np.isclose(y_out.m, min_val_same_u.m, atol=self.atol, rtol=self.rtol)
        y_out[np.where(already_close_to_min_val)] = min_val_same_u

        below_min = y_out < self.min_val
        below_min_in_group = get_group_sums(
            x_bounds=x_bounds_out,
            vals=below_min,
            group_bounds=x_bounds_target,
        )
        y_out_group_index = get_group_indexes(x_bounds=x_bounds_out, group_bounds=x_bounds_target)

        min_val_applier = self.get_min_val_applier(self)

        iterh = np.where(below_min_in_group > 0)[0]
        if self.progress_bar:
            # # TODO: switch to optional pattern
            # tqdman = get_optional_dependency("tqdm.auto")
            import tqdm.auto as tqdman

            iterh = tqdman.tqdm(iterh, desc="Updating intervals where the solution is less than the minimum value")

        for below_min_group_idx in iterh:
            below_min_group_lai_kaplan_idx = below_min_group_idx + 1

            interval_indexer = np.where(y_out_group_index == below_min_group_idx)
            interval_vals = y_out[interval_indexer]

            x_bounds_out_interval = x_bounds_out[interval_indexer[0][0] : interval_indexer[0][-1] + 2]

            x_bounds_in_interval = x_bounds_target[below_min_group_idx : below_min_group_idx + 2]
            y_in_interval = target[[below_min_group_idx]]

            left_bound_val_interval = control_points_y[below_min_group_lai_kaplan_idx]
            right_bound_val_interval = control_points_y[below_min_group_lai_kaplan_idx + 1]

            interval_vals_updated = min_val_applier.iterate_to_solution(
                starting_values=interval_vals,
                x_bounds_out=x_bounds_out_interval,
                x_bounds_in=x_bounds_in_interval,
                y_in=y_in_interval,
                left_bound_val=left_bound_val_interval,
                right_bound_val=right_bound_val_interval,
                min_val=self.min_val,
            )

            y_out[interval_indexer] = interval_vals_updated

        return y_out
