from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import dask.array as da

import numpy as np
import xarray as xr
from typing import Union
from geo_skeletons import dask_computations

OFFSET = {"from": 180, "to": 0}


def convert(data, in_type: str, out_type: str):
    """Converts one directional type to another.

    Possible types are:
    'from', 'to' and 'math'
    """
    if out_type is None:
        return data
    if in_type is None:
        raise ValueError("Cannot convert 'dir_type' for a non-directional variable!")
    data = convert_to_math_dir(data, dir_type=in_type)
    data = convert_from_math_dir(data, dir_type=out_type)
    return data


def convert_to_math_dir(data, dir_type: str):
    """Converts data to mathematical convetion (radians, east=0, north = pi/2)"""
    if dir_type == "math":  # Convert to mathematical convention
        return data
    math_dir = (90 - data + OFFSET[dir_type]) * np.pi / 180
    math_dir = dask_computations.mod(math_dir, 2 * np.pi)
    mask = dask_computations.undask_me(math_dir <= np.pi)
    if isinstance(mask, xr.DataArray):
        mask = mask.data
    correction = np.full(mask.shape, 2 * np.pi)
    correction[mask] = 0

    if isinstance(math_dir, xr.DataArray):
        # squeeze is needed for it to work with dask arrays
        math_dir.data = math_dir.data - correction
    else:
        math_dir = math_dir - correction
    return math_dir


def convert_from_math_dir(data, dir_type: str):
    """Converts data from mathematical convention"""
    if dir_type == "math":
        return data

    data = 90 - data * 180 / np.pi + OFFSET[dir_type]
    return dask_computations.mod(data, 360)


def compute_magnitude(
    x: Union[np.ndarray, da.array, xr.DataArray],
    y: Union[np.ndarray, da.array, xr.DataArray],
) -> Union[np.ndarray, da.array, xr.DataArray]:
    """Computes magnitudes (norms) of two variables"""
    if x is None or y is None:
        return None
    return (x**2 + y**2) ** 0.5


def compute_math_direction(
    x: Union[np.ndarray, da.array, xr.DataArray],
    y: Union[np.ndarray, da.array, xr.DataArray],
) -> Union[np.ndarray, da.array, xr.DataArray]:
    """Computes direcetion of two component variables.

    Result given in mathemetical convention (radians, east=0, north = pi/2)"""
    if x is None or y is None:
        return None

    math_dir = dask_computations.arctan2(y, x)
    return math_dir
