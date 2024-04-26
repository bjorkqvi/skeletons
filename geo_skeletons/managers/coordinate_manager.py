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
        self.x_str = None
        self.y_str = None
        self._added_coords = {}

        self._added_vars = {}
        self._added_magnitudes = {}
        self._added_directions = {}
        self._added_masks = {}

        self.set_initial_coords(initial_coords)
        self.set_initial_vars(initial_vars)

        # This will be used by decorators to make a deepcopy of the manager for different classes
        self.initial_state = True

    def is_initialized(self) -> bool:
        return self.x_str is not None and self.y_str is not None

    def is_cartesian(self) -> bool:
        """Checks if the grid is cartesian (True) or spherical (False)."""
        if self.x_str == "x" and self.y_str == "y":
            return True
        elif self.x_str == "lon" and self.y_str == "lat":
            return False
        raise ValueError(
            f"Expected x- and y string to be either 'x' and 'y' or 'lon' and 'lat', but they were {self.x_str} and {self.y_str}"
        )

    def add_var(self, data_var: DataVar) -> None:
        if self.get(data_var.name) is not None:
            raise VariableExistsError(data_var.name)
        self._added_vars[data_var.name] = data_var

    def add_mask(self, grid_mask: GridMask) -> None:
        if self.get(grid_mask.name) is not None:
            raise VariableExistsError(grid_mask.name)
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
        self._added_masks[grid_mask.name] = grid_mask

    def triggers(self, name: str) -> list[str]:
        return [
            mask for mask in self._added_masks.values() if mask.triggered_by == name
        ]

        # list_of_computations = self.triggers.get(triggered_by, [])
        # list_of_computations.append((name, valid_range, range_inclusive))
        # self.triggers[triggered_by] = list_of_computations

    def add_coord(self, coord: Coordinate) -> str:
        """Add a coordinate that the Skeleton will use."""
        if self.get(coord.name) is not None:
            raise VariableExistsError(coord.name)
        self._added_coords[coord.name] = coord

    def add_magnitude(self, magnitude: Magnitude) -> None:
        if self.get(magnitude.name) is not None:
            raise VariableExistsError(magnitude.name)
        self._added_magnitudes[magnitude.name] = magnitude

    def add_direction(self, direction: Direction) -> None:
        if self.get(direction.name) is not None:
            raise VariableExistsError(direction.name)
        self._added_directions[direction.name] = direction

    def set_initial_vars(self, initial_vars: list) -> None:
        """Set dictionary containing the initial variables of the Skeleton"""
        if not isinstance(initial_vars, list):
            raise ValueError("initial_vars needs to be a dict of DataVar's!")
        ## Class has x/y set automatically, but instance might change to lon/lat
        for var in list(self._added_vars.keys()):
            if var in SPATIAL_COORDS:
                del self._added_vars[var]
        for var in initial_vars:
            self._added_vars[var.name] = var

    def set_initial_coords(self, initial_coords: list) -> None:
        """Set dictionary containing the initial coordinates of the Skeleton"""
        if not isinstance(initial_coords, list):
            raise ValueError("initial_coords needs to be a list of strings!")
        ## Class has x/y set automatically, but instance might change to lon/lat
        for coord in list(self._added_coords.keys()):
            if coord in SPATIAL_COORDS:
                del self._added_coords[coord]
        for coord in initial_coords:
            self._added_coords[coord.name] = coord

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
            coords = self._added_coords.values()
        elif coord_group == "nonspatial":
            coords = [
                coord
                for coord in self._added_coords.values()
                if coord.coord_group != "spatial"
            ]
        elif coord_group == "grid":
            coords = [
                coord
                for coord in self._added_coords.values()
                if coord.coord_group in [coord_group, "spatial"]
            ]
        else:
            coords = [
                coord
                for coord in self._added_coords.values()
                if coord.coord_group == coord_group
            ]

        return move_time_and_spatial_to_front([coord.name for coord in coords])

    def masks(self, coord_group: str = "all") -> list[str]:
        """Returns list of masks that have been added to a specific coord group.

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
            masks = self._added_masks.values()
        elif coord_group == "nonspatial":
            masks = [
                mask
                for mask in self._added_masks.values()
                if mask.coord_group != "spatial"
            ]
        elif coord_group == "grid":
            masks = [
                mask
                for mask in self._added_masks.values()
                if mask.coord_group in [coord_group, "spatial"]
            ]
        else:
            masks = [
                mask
                for mask in self._added_masks.values()
                if mask.coord_group == coord_group
            ]

        return [mask.name for mask in masks]

    def data_vars(self, coord_group: str = "nonspatial") -> list[str]:
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
            vars = self._added_vars.values()
        elif coord_group == "nonspatial":
            vars = [
                var for var in self._added_vars.values() if var.coord_group != "spatial"
            ]
        elif coord_group == "grid":
            vars = [
                var
                for var in self._added_vars.values()
                if var.coord_group in [coord_group, "spatial"]
            ]
        else:
            vars = [
                var
                for var in self._added_vars.values()
                if var.coord_group == coord_group
            ]

        return move_time_and_spatial_to_front([var.name for var in vars if var.name])

    def magnitudes(self, coord_group: str = "all") -> list[str]:
        """Returns list of magnitudes that have been added to a specific coord group.

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
            vars = self._added_magnitudes.values()
        elif coord_group == "nonspatial":
            vars = [
                var
                for var in self._added_magnitudes.values()
                if var.x.coord_group != "spatial"
            ]
        elif coord_group == "grid":
            vars = [
                var
                for var in self._added_magnitudes.values()
                if var.x.coord_group in [coord_group, "spatial"]
            ]
        else:
            vars = [
                var
                for var in self._added_magnitudes.values()
                if var.x.coord_group == coord_group
            ]

        return [var.name for var in vars]

    def directions(self, coord_group: str = "all") -> list[str]:
        """Returns list of directions that have been added to a specific coord group.

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
            vars = self._added_directions.values()
        elif coord_group == "nonspatial":
            vars = [
                var
                for var in self._added_directions.values()
                if var.x.coord_group != "spatial"
            ]
        elif coord_group == "grid":
            vars = [
                var
                for var in self._added_directions.values()
                if var.x.coord_group in [coord_group, "spatial"]
            ]
        else:
            vars = [
                var
                for var in self._added_directions.values()
                if var.x.coord_group == coord_group
            ]

        return [var.name for var in vars]

    def all_objects(self, coord_group: str = "all") -> list[str]:
        list_of_objects = (
            self.data_vars(coord_group)
            + self.coords(coord_group)
            + self.magnitudes(coord_group)
            + self.directions(coord_group)
            + self.masks(coord_group)
        )
        return list_of_objects

    def coord_group(self, var: str) -> str:
        """Returns the coordinate group that a variable/mask is defined over.
        The coordinates can then be retrived using the group by the method .coords()"""
        vars = [v for v in self._added_vars.values() if v.name == var]
        masks = [v for v in self._added_masks.values() if v.name == var]
        mags = [v for v in self._added_magnitudes.values() if v.name == var]
        dirs = [v for v in self._added_directions.values() if v.name == var]
        all_vars = vars + masks + mags + dirs
        # coord_group = var_coords or mask_coords or mag_coords or dir_coords
        if not all_vars:
            raise KeyError(f"Cannot find the data {var}!")

        return all_vars[0].coord_group

    def get(self, var: str):
        return (
            self._added_coords.get(var)
            or self._added_vars.get(var)
            or self._added_magnitudes.get(var)
            or self._added_directions.get(var)
            or self._added_masks.get(var)
        )

    def meta_parameter(self, var: str) -> MetaParameter:
        param = self.get(var)
        if param is None:
            return None
        return param.meta

    def default_value(self, var: str):
        param = self.get(var)
        if param is None:
            return None
        if not hasattr(param, "default_value"):
            return None
        return param.default_value

    # def is_settable(self, name: str) -> bool:
    #     """Check if the variable etc. is allowed to be set (i.e. is not a magnitude, opposite mask etc.)"""
    #     return (
    #         self._added_vars().get(name) is not None
    #         or self._added_masks().get(name) is not None
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
    if "lon" in coord_list:
        coord_list.insert(0, coord_list.pop(coord_list.index("lon")))
    if "lat" in coord_list:
        coord_list.insert(0, coord_list.pop(coord_list.index("lat")))
    if "time" in coord_list:
        coord_list.insert(0, coord_list.pop(coord_list.index("time")))
    return coord_list
