"""
Mean preserving interpolation

This is a surprisingly tricky thing to do.
Hence, this module is surprisingly large.

TODO: split this out into a standalone package
using the tests from CMIP-GHG-Concentration-Generation
"""

from __future__ import annotations

from cmip7_scenariomip_ghg_generation.mean_preserving_interpolation.annual_to_monthly import (
    interpolate_annual_mean_to_monthly,
)
from cmip7_scenariomip_ghg_generation.mean_preserving_interpolation.core import (
    MeanPreservingInterpolationAlgorithmLike,
    mean_preserving_interpolation,
)
from cmip7_scenariomip_ghg_generation.mean_preserving_interpolation.lai_kaplan import (
    LaiKaplanInterpolator,
)

__all__ = [
    "LaiKaplanInterpolator",
    "MeanPreservingInterpolationAlgorithmLike",
    "interpolate_annual_mean_to_monthly",
    "mean_preserving_interpolation",
]
