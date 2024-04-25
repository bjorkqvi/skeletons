from geo_parameters.metaparameter import MetaParameter
from typing import Union
from geo_parameters.grid import Lon, Lat, X, Y, Inds
import numpy as np
import dask.array as da
from geo_skeletons.errors import VariableExistsError
import geo_parameters as gp
from geo_skeletons.managers.dask_manager import DaskManager
import xarray as xr
from geo_skeletons.variables import DataVar, Magnitude, Direction, GridMask, Coordinate

meta_parameters = {"lon": Lon, "lat": Lat, "x": X, "y": Y, "inds": Inds}

SPATIAL_COORDS = ["y", "x", "lat", "lon", "inds"]
OFFSET = {"from": 180, "to": 0}


class CoordinateManager:
    """Keeps track of coordinates and data variables that are added to classes
    by the decorators."""

    def __init__(self, initial_coords, initial_vars) -> None:
        # self._coords = {}
        # self._coords["grid"] = []
        # self._coords["gridpoint"] = []
        # self._coords["initial"] = []

        self.added_coords = {}
        # self._vars = {}
        # self._vars["added"] = {}
        # self._vars["initial"] = {}

        # Refactoring one
        self.added_vars = {}
        self.added_magnitudes = {}
        self.added_directions = {}
        self.added_masks = {}
        self.magnitudes = {}
        self.directions = {}

        # self.meta_coords: dict[str, MetaParameter] = {}
        # self.meta_vars: dict[str, MetaParameter] = {}
        # self.dir_vars: dict[str, str] = {}
        # self.meta_masks: dict[str, MetaParameter] = {}
        # self.meta_magnitudes: dict[str, MetaParameter] = {}
        # self.meta_directions: dict[str, MetaParameter] = {}

        # E.g. creating a land-mask might be triggered by setting a bathymetry or hs variable
        # self.triggers: dict[str, list[tuple[str, tuple[float], tuple[bool]]]] = {}

        self._used_names = []

        self.set_initial_coords(initial_coords)
        self.set_initial_vars(initial_vars)

        # This will be used by decorators to make a deepcopy of the manager for different classes
        self.initial_state = True

    def add_var(self, data_var: DataVar) -> None:
        if data_var.name in self._used_names:
            raise VariableExistsError(data_var.name)
        self.added_vars[data_var.name] = data_var
        self._used_names.append(data_var.name)

    def add_mask(self, grid_mask: GridMask) -> None:
        if grid_mask.triggered_by:
            grid_mask.valid_range = tuple(
                [np.inf if r is None else r for r in grid_mask.valid_range]
            )
        # if len(grid_mask.valid_range) != 2:
        #     raise ValueError(f"valid_rang has to be of length 2 (upper, lower)!")

        if isinstance(grid_mask.range_inclusive, bool):
            grid_mask.range_inclusive = (
                grid_mask.range_inclusive,
                grid_mask.range_inclusive,
            )
        self.added_masks[grid_mask.name] = grid_mask

        # list_of_computations = self.triggers.get(triggered_by, [])
        # list_of_computations.append((name, valid_range, range_inclusive))
        # self.triggers[triggered_by] = list_of_computations

    def add_coord(self, coord: Coordinate) -> str:
        """Add a coordinate that the Skeleton will use."""
        if coord.name in self._used_names:
            raise VariableExistsError(coord.name)
        self.added_coords[coord.name] = coord
        self._used_names.append(coord.name)

    def add_magnitude(self, magnitude: Magnitude) -> None:
        if magnitude.name in self._used_names:
            raise VariableExistsError(magnitude.name)
        self.added_magnitudes[magnitude.name] = magnitude
        self._used_names.append(magnitude.name)

    def add_direction(self, direction: Direction) -> None:
        if direction.name in self._used_names:
            raise VariableExistsError(direction.name)
        self.added_directions[direction.name] = direction
        self._used_names.append(direction.name)

    def set_initial_vars(self, initial_vars: list) -> None:
        """Set dictionary containing the initial variables of the Skeleton"""
        if not isinstance(initial_vars, list):
            raise ValueError("initial_vars needs to be a dict of DataVar's!")
        ## Class has x/y set automatically, but instance might change to lon/lat
        for var in list(self.added_vars.keys()):
            if var in SPATIAL_COORDS:
                del self.added_vars[var]
        for var in initial_vars:
            self.added_vars[var.name] = var

    def set_initial_coords(self, initial_coords: list) -> None:
        """Set dictionary containing the initial coordinates of the Skeleton"""
        if not isinstance(initial_coords, list):
            raise ValueError("initial_coords needs to be a list of strings!")
        ## Class has x/y set automatically, but instance might change to lon/lat
        for coord in list(self.added_coords.keys()):
            if coord in SPATIAL_COORDS:
                del self.added_coords[coord]
        for coord in initial_coords:
            self.added_coords[coord.name] = coord

    def coords(self, coord_group: str = "all") -> list[str]:
        """Returns list of coordinats that have been added to a specific coord group.

        'all': All added coordinates
        'spatial': spatial coords (e.g. inds, or lat/lon)
        'nonspatial': All EXCEPT spatial coords (e.g. inds, or lat/lon)
        'grid': coordinates for the grid (e.g. z, time)
        'gridpoint': coordinates for a grid point (e.g. frequency, direcion or time)
        """
        if coord_group not in ["all", "spatial", "nonspatial", "grid", "gridpoint"]:
            print(
                "Coord group needs to be 'all', 'spatial', 'nonspatial','grid' or 'gridpoint'."
            )
            return None

        if coord_group == "all":
            coords = self.added_coords.values()
        elif coord_group == "nonspatial":
            coords = [
                coord
                for coord in self.added_coords.values()
                if coord.coord_group != "spatial"
            ]
        elif coord_group == "grid":
            coords = [
                coord
                for coord in self.added_coords.values()
                if coord.coord_group in [coord_group, "spatial"]
            ]
        else:
            coords = [
                coord
                for coord in self.added_coords.values()
                if coord.coord_group == coord_group
            ]

        return move_time_and_spatial_to_front([coord.name for coord in coords])

    def data_vars(self, coord_group: str = "all") -> list[str]:
        """Returns list of variables that have been added to a specific coord group.

        'all': All added coordinates
        'spatial': spatial coords (e.g. inds, or lat/lon)
        'nonspatial': All EXCEPT spatial coords (e.g. inds, or lat/lon)
        'grid': coordinates for the grid (e.g. z, time)
        'gridpoint': coordinates for a grid point (e.g. frequency, direcion or time)
        """
        if coord_group not in ["all", "spatial", "nonspatial", "grid", "gridpoint"]:
            print(
                "Coord group needs to be 'all', 'spatial', 'nonspatial','grid' or 'gridpoint'."
            )
            return None

        if coord_group == "all":
            vars = self.added_vars.values()
        elif coord_group == "nonspatial":
            vars = [
                var for var in self.added_vars.values() if var.coord_group != "spatial"
            ]
        elif coord_group == "grid":
            vars = [
                var
                for var in self.added_vars.values()
                if var.coord_group in [coord_group, "spatial"]
            ]
        else:
            vars = [
                var
                for var in self.added_vars.values()
                if var.coord_group == coord_group
            ]

        return [var.name for var in vars]

    def coord_group(self, var: str) -> str:
        """Returns the coordinate group that a variable/mask is defined over.
        The coordinates can then be retrived using the group by the method .coords()"""
        vars = [v for v in self.added_vars.values() if v.name == var]
        masks = [v for v in self.added_masks.values() if v.name == var]
        mags = [v for v in self.added_magnitudes.values() if v.name == var]
        dirs = [v for v in self.added_directions.values() if v.name == var]
        all_vars = vars + masks + mags + dirs
        # coord_group = var_coords or mask_coords or mag_coords or dir_coords
        if not all_vars:
            raise KeyError(f"Cannot find the data {var}!")

        return all_vars[0].coord_group

    def get_added(self, var: str):
        return (
            self.added_coords.get(var)
            or self.added_vars.get(var)
            or self.added_magnitudes.get(var)
            or self.added_directions.get(var)
            or self.added_masks.get(var)
        )

    def meta_parameter(self, var: str) -> MetaParameter:
        param = self.get_added(var)
        if param is None:
            return None
        return param.meta

    def default_value(self, var: str):
        param = self.get_added(var)
        if param is None:
            return None
        if not hasattr(param, "default_value"):
            return None
        return param.default_value

    # def is_settable(self, name: str) -> bool:
    #     """Check if the variable etc. is allowed to be set (i.e. is not a magnitude, opposite mask etc.)"""
    #     return (
    #         self.added_vars().get(name) is not None
    #         or self.added_masks().get(name) is not None
    #     )

    def convert_to_math_dir(self, data, dir_type: str):
        if dir_type == "math":  # Convert to mathematical convention
            return data
        math_dir = (90 - data + OFFSET[dir_type]) * np.pi / 180
        dask_manager = DaskManager()
        math_dir = dask_manager.mod(math_dir, 2 * np.pi)
        mask = dask_manager.undask_me(math_dir > np.pi)
        if isinstance(mask, xr.DataArray):
            mask = mask.data
        if isinstance(math_dir, xr.DataArray):
            math_dir.data[mask] = math_dir.data[mask] - 2 * np.pi
        else:
            math_dir[mask] = math_dir[mask] - 2 * np.pi
        return math_dir

    def convert_from_math_dir(self, data, dir_type: str):
        if dir_type == "math":
            return data

        data = 90 - data * 180 / np.pi + OFFSET[dir_type]
        return DaskManager().mod(data, 360)

    def compute_magnitude(self, x, y):
        if x is None or y is None:
            return None
        return (x**2 + y**2) ** 0.5

    def compute_math_direction(self, x, y):
        if x is None or y is None:
            return None

        dask_manager = DaskManager()
        math_dir = dask_manager.arctan2(y, x)
        return math_dir


def move_time_and_spatial_to_front(coord_list) -> list[str]:
    if "inds" in coord_list:
        coord_list.insert(0, coord_list.pop(coord_list.index("inds")))
    if "x" in coord_list:
        coord_list.insert(0, coord_list.pop(coord_list.index("x")))
    if "y" in coord_list:
        coord_list.insert(0, coord_list.pop(coord_list.index("y")))
    if "time" in coord_list:
        coord_list.insert(0, coord_list.pop(coord_list.index("time")))
    return coord_list
