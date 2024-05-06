from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .coordinate_manager import CoordinateManager
    import dask.array as da

from .dask_manager import DaskManager
import numpy as np
import xarray as xr
from typing import Union
from geo_skeletons import dask_computations

OFFSET = {"from": 180, "to": 0}


class DirTypeManager:
    def __init__(self, coord_manager: CoordinateManager) -> None:
        self.coord_manager = coord_manager

    def convert(self, data, in_type: str, out_type: str):
        """Converts one directional type to another.

        Possible types are:
        'from', 'to' and 'math'
        """
        if out_type is None:
            return data
        if in_type is None:
            raise ValueError(
                "Cannot convert 'dir_type' for a non-directional variable!"
            )
        data = self.convert_to_math_dir(data, dir_type=in_type)
        data = self.convert_from_math_dir(data, dir_type=out_type)
        return data

    def get_dir_type(self, name: str) -> str:
        """Get the dir_type of a variable if possible"""
        obj = self.coord_manager.get(name)
        if obj is None:
            return None
        if not hasattr(obj, "dir_type"):
            return None
        return obj.dir_type

    @staticmethod
    def convert_to_math_dir(data, dir_type: str):
        """Converts data to mathematical convetion (radians, east=0, north = pi/2)"""
        if dir_type == "math":  # Convert to mathematical convention
            return data
        math_dir = (90 - data + OFFSET[dir_type]) * np.pi / 180
        math_dir = dask_computations.mod(math_dir, 2 * np.pi)
        mask = dask_computations.undask_me(math_dir > np.pi)
        if isinstance(mask, xr.DataArray):
            mask = mask.data
        if isinstance(math_dir, xr.DataArray):
            math_dir.data[mask] = math_dir.data[mask] - 2 * np.pi
        else:
            math_dir[mask] = math_dir[mask] - 2 * np.pi
        return math_dir

    @staticmethod
    def convert_from_math_dir(data, dir_type: str):
        """Converts data from mathematical convention"""
        if dir_type == "math":
            return data

        data = 90 - data * 180 / np.pi + OFFSET[dir_type]
        return dask_computations.mod(data, 360)

    @staticmethod
    def compute_magnitude(
        x: Union[np.ndarray, da.array, xr.DataArray],
        y: Union[np.ndarray, da.array, xr.DataArray],
    ) -> Union[np.ndarray, da.array, xr.DataArray]:
        """Computes magnitudes (norms) of two variables"""
        if x is None or y is None:
            return None
        return (x**2 + y**2) ** 0.5

    @staticmethod
    def compute_math_direction(
        x: Union[np.ndarray, da.array, xr.DataArray],
        y: Union[np.ndarray, da.array, xr.DataArray],
    ) -> Union[np.ndarray, da.array, xr.DataArray]:
        """Computes direcetion of two variables.

        Result given in mathemetical convention (radians, east=0, north = pi/2)"""
        if x is None or y is None:
            return None

        math_dir = dask_computations.arctan2(y, x)
        return math_dir
