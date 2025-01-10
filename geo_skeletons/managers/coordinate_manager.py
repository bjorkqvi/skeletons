from geo_parameters.metaparameter import MetaParameter
import geo_parameters as gp
import numpy as np
from geo_skeletons.errors import VariableExistsError

from geo_skeletons.variables import DataVar, Magnitude, Direction, GridMask, Coordinate
from typing import Union
from geo_skeletons.errors import StaticSkeletonError

from geo_skeletons.variable_archive import SPATIAL_COORDS


class CoordinateManager:
    """Keeps track of coordinates and data variables that are added to classes
    by the decorators."""

    def __init__(
        self,
        initial_coords: list[Coordinate],
        initial_vars: list[DataVar],
        metadata_manager,
    ) -> None:
        self.x_str = None
        self.y_str = None
        self._added_coords = {}
        self._added_vars = {}
        self._added_magnitudes = {}
        self._added_directions = {}
        self._added_masks = {}
        self._added_mask_points = {}

        self._set_initial_coords = [c.name for c in initial_coords]
        self._set_initial_vars = [v.name for v in initial_vars]

        self.set_initial_coords(initial_coords)
        self.set_initial_vars(initial_vars)

        self.meta = metadata_manager

    def _is_initialized(self) -> bool:
        """Check if the Dataset had been initialized"""
        return self.x_str is not None and self.y_str is not None

    def _is_altered(self) -> bool:
        """Check if the coordinate structure has been altered"""
        p1 = set(self.coords("all")) == set(self._set_initial_coords)
        p2 = set(self.data_vars("all")) == set(self._set_initial_vars)
        p3 = self._added_magnitudes == {}
        p4 = self._added_directions == {}
        p5 = self._added_masks == {}
        p6 = self._added_mask_points == {}
        return not (p1 and p2 and p3 and p4 and p5 and p6)

    def is_cartesian(self) -> bool:
        """Checks if the grid is cartesian"""
        if self.x_str == "x" and self.y_str == "y":
            return True
        elif self.x_str == "lon" and self.y_str == "lat":
            return False
        raise ValueError(
            f"Expected x- and y string to be either 'x' and 'y' or 'lon' and 'lat', but they were {self.x_str} and {self.y_str}"
        )

    def is_spherical(self) -> bool:
        """Checks if the grid is cartesian"""
        return not self.is_cartesian()

    def add_var(self, data_var: DataVar) -> None:
        """Adds a data variable to the structure"""
        if self.get(data_var.name) is not None:
            raise VariableExistsError(data_var.name)
        self._added_vars[data_var.name] = data_var

        # Set metadata from MetaParameter if it is provided
        if data_var.meta is not None:
            self.meta.append(data_var.meta.meta_dict(), data_var.name)

    def add_mask(self, grid_mask: GridMask) -> None:
        """Adds a mask to the structure"""
        if self.get(grid_mask.name) is not None:
            raise VariableExistsError(grid_mask.name)
        if grid_mask.triggered_by:
            grid_mask.valid_range = tuple(
                [np.inf if r is None else r for r in grid_mask.valid_range]
            )
        if isinstance(grid_mask.range_inclusive, bool):
            grid_mask.range_inclusive = (
                grid_mask.range_inclusive,
                grid_mask.range_inclusive,
            )
        self._added_masks[grid_mask.name] = grid_mask
        self._added_mask_points[grid_mask.point_name] = grid_mask

        # Set metadata from MetaParameter if it is provided
        if grid_mask.meta is not None:
            self.meta.append(grid_mask.meta.meta_dict(), grid_mask.name)

    def triggers(self, name: str) -> list[str]:
        """Returns the masks that are triggered by a specific variable"""
        return [
            mask for mask in self._added_masks.values() if mask.triggered_by == name
        ]

    def add_coord(self, coord: Coordinate) -> str:
        """Adds a coordinate to the structure"""
        if self.get(coord.name) is not None:
            raise VariableExistsError(coord.name)
        self._added_coords[coord.name] = coord

        # Set metadata from MetaParameter if it is provided
        if coord.meta is not None:
            self.meta.append(coord.meta.meta_dict(), coord.name)

    def add_magnitude(self, magnitude: Magnitude) -> None:
        """Adds a magnitude to the structure"""
        if self.get(magnitude.name) is not None:
            raise VariableExistsError(magnitude.name)
        self._added_magnitudes[magnitude.name] = magnitude

        # Set metadata from MetaParameter if it is provided
        if magnitude.meta is not None:
            self.meta.append(magnitude.meta.meta_dict(), magnitude.name)

    def add_direction(self, direction: Direction) -> None:
        """Adds a direction to the structure"""
        if self.get(direction.name) is not None:
            raise VariableExistsError(direction.name)
        self._added_directions[direction.name] = direction

        # Set metadata from MetaParameter if it is provided
        if direction.meta is not None:
            self.meta.append(direction.meta.meta_dict(), direction.name)

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

    def coords(self, coord_group: str = "all", cartesian: bool = None) -> list[str]:
        """Returns list of coordinats that have been added to a specific coord group.

        'all': All added coordinates
        'spatial': spatial coords (e.g. inds, or lat/lon)
        'nonspatial': All EXCEPT spatial coords (e.g. inds, or lat/lon, x/y)
        'init': coordinates needed to initialize Skeleton: All coords (including lat/lon, x/y in PointSkeleton) but without 'inds'
            Provide cartesian = True/False to only get x/y or lon/lat
        'grid': coordinates for the grid (e.g. z, time)
        'gridpoint': coordinates for a grid point (e.g. frequency, direcion or time)
        """
        if coord_group not in [
            "all",
            "spatial",
            "nonspatial",
            "grid",
            "gridpoint",
            "init",
        ]:
            raise ValueError(
                "Coord group needs to be 'all', 'spatial', 'nonspatial', 'grid', 'gridpoint' or 'init'."
            )

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
        elif coord_group == "init":
            coords = list(set(self.coords()) - set(["inds"])) + self.data_vars(
                "spatial"
            )
            if (
                not self._is_initialized()
            ):  # Can use either x/y or lon/lat if it has not yet been determined
                coords = coords + ["lon", "lat"]
            if cartesian is not None:
                if cartesian:
                    coords = list(set(coords) - {"lon", "lat"})
                else:
                    coords = list(set(coords) - {"x", "y"})
        else:
            coords = [
                coord
                for coord in self._added_coords.values()
                if coord.coord_group == coord_group
            ]

        if coord_group != "init":
            coords = [coord.name for coord in coords]

        return move_time_and_spatial_to_front(coords)

    def masks(self, coord_group: str = "all") -> list[str]:
        """Returns list of masks that have been added to a specific coord group.

        'all': All added coordinates
        'spatial': spatial coords (e.g. inds, or lat/lon)
        'nonspatial': All EXCEPT spatial coords (e.g. inds, or lat/lon, x/y)
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

    def mask_points(self, coord_group: str = "all") -> list[str]:
        """Returns list of mask_points that have been added to a specific coord group.

        'all': All added coordinates
        'spatial': spatial coords (e.g. inds, or lat/lon)
        'nonspatial': All EXCEPT spatial coords (e.g. inds, or lat/lon, x/y)
        'grid': coordinates for the grid (e.g. z, time)
        'gridpoint': coordinates for a grid point (e.g. frequency, direcion or time)
        """
        if coord_group not in ["all", "spatial", "nonspatial", "grid", "gridpoint"]:
            print(
                "Coord group needs to be 'all', 'spatial', 'nonspatial','grid' or 'gridpoint'."
            )
            return None

        if coord_group == "all":
            masks = self._added_mask_points.values()
        elif coord_group == "nonspatial":
            masks = [
                mask
                for mask in self._added_mask_points.values()
                if mask.coord_group != "spatial"
            ]
        elif coord_group == "grid":
            masks = [
                mask
                for mask in self._added_mask_points.values()
                if mask.coord_group in [coord_group, "spatial"]
            ]
        else:
            masks = [
                mask
                for mask in self._added_mask_points.values()
                if mask.coord_group == coord_group
            ]

        return [mask.point_name for mask in masks]

    def _mask_is_primary(self, name: str) -> bool:
        """Checks if a mask is a primary mask or an mask defined as an opposite to a primary mask"""
        mask = self._added_masks.get(f"{name}")
        if mask is None:
            raise ValueError(f"No mask: '{name}'!")
        return mask.primary_mask

    def _find_primary_mask(self, name: str) -> bool:
        """Finds the name of the primary mask to a given opposite_mask"""
        masks = self.masks()
        for mask in masks:
            if self._added_masks.get(mask).opposite_mask is not None:
                if self._added_masks.get(mask).opposite_mask.name == name:
                    return mask
        return None

    def data_vars(self, coord_group: str = "nonspatial") -> list[str]:
        """Returns list of variables that have been added to a specific coord group.

        'all': All added coordinates
        'spatial': spatial coords (e.g. inds, or lat/lon)
        'nonspatial': All EXCEPT spatial coords (e.g. inds, or lat/lon, x/y)
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
        'nonspatial': All EXCEPT spatial coords (e.g. inds, or lat/lon, x/y)
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
        'nonspatial': All EXCEPT spatial coords (e.g. inds, or lat/lon, x/y)
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
        """Returns a list of all objects for the given coord_group"""
        list_of_objects = (
            self.data_vars(coord_group)
            + self.coords(coord_group)
            + self.magnitudes(coord_group)
            + self.directions(coord_group)
            + self.masks(coord_group)
        )
        return list_of_objects

    def non_coord_objects(self, coord_group: str = "all") -> list[str]:
        """Returns a list of all objects for given coord_group that are not coords or spatial data_vars (e.g. 'x' in PointSkeleton)"""
        not_accepted = set(self.coords("all") + self.data_vars("spatial"))
        all_objects = set(self.all_objects(coord_group))
        accepted = all_objects - not_accepted
        return list(accepted)

    def coord_group(self, var: str) -> str:
        """Returns the coordinate group that a variable/mask is defined over.
        The coordinates can then be retrived using the group by the method .coords()"""
        coords = [v for v in self._added_coords.values() if v.name == var]
        vars = [v for v in self._added_vars.values() if v.name == var]
        masks = [v for v in self._added_masks.values() if v.name == var]
        mags = [v for v in self._added_magnitudes.values() if v.name == var]
        dirs = [v for v in self._added_directions.values() if v.name == var]
        all_vars = coords + vars + masks + mags + dirs
        if not all_vars:
            raise KeyError(f"Cannot get coord_group for unknown variable {var}!")

        return all_vars[0].coord_group

    def get(
        self, var: str
    ) -> Union[Coordinate, DataVar, Magnitude, Direction, GridMask]:
        """Returns a Coordinate, data variabel, magnitude, direction of mask with a given name"""
        return (
            self._added_coords.get(var)
            or self._added_vars.get(var)
            or self._added_magnitudes.get(var)
            or self._added_directions.get(var)
            or self._added_masks.get(var)
        )

    def meta_parameter(self, var: str) -> Union[MetaParameter, None]:
        """Returns a metaparameter for a given parameter"""
        param = self.get(var)
        if param is None:
            return None
        return param.meta

    def default_value(self, var: str) -> Union[int, float, None]:
        """Returns default value for a given parameter"""
        param = self.get(var)
        if param is None:
            return None
        if not hasattr(param, "default_value"):
            return None
        return param.default_value

    def get_dir_type(self, name: str) -> str:
        """Get the dir_type of a variable if possible"""
        obj = self.get(name)
        if obj is None:
            return None
        if not hasattr(obj, "dir_type"):
            return None
        return obj.dir_type

    def find_cf(self, standard_name: str) -> list[str]:
        """Finds the variable names that have the given standard name"""
        names = []

        for name in self.all_objects():
            obj = self.get(name)
            if obj.meta is None:
                continue
            if (
                obj.meta.standard_name() == standard_name
                or obj.meta.standard_name(alias=True) == standard_name
            ):
                names.append(obj.name)

        return names

    def find(self, param: Union[MetaParameter, str]) -> list[str]:
        """Finds the name of a parameter based on the standard name or a MetaParameter

        If several matches exist and a MetaParameter is provided, a name match is attempted.
        """
        if gp.is_gp(param):
            std_name = param.standard_name()
        else:
            std_name = param

        names = self.find_cf(std_name)

        if len(names) < 2 or not gp.is_gp_instance(param):
            return names
        else:
            clean_names = [n for n in names if n == param.name]
            if clean_names:
                return clean_names
            return names

    def __repr__(self):
        def string_of_coords(list_of_coords) -> str:
            if not list_of_coords:
                return ""
            string = "("
            for c in list_of_coords:
                string += f"{c}, "
            string = string[:-2]
            string += ")"
            return string

        string = f"{' Coordinate groups ':-^80}" + "\n"
        string += f"{'Spatial:':12}"

        string += string_of_coords(self.coords("spatial")) or "*empty*"
        string += f"\n{'Grid:':12}"
        string += string_of_coords(self.coords("grid")) or "*empty*"
        string += f"\n{'Gridpoint:':12}"
        string += string_of_coords(self.coords("gridpoint")) or "*empty*"

        string += f"\n{'All:':12}"
        string += string_of_coords(self.coords("all")) or "*empty*"

        string += f"\n{' Data ':-^80}"
        string += "\n" + "Variables:"
        if self.data_vars():
            max_len = len(max(self.data_vars(), key=len))
            for var in self.data_vars():
                string += f"\n    {var:{max_len+2}}"
                string += string_of_coords(self.coords(self.coord_group(var)))
                string += f":  {self.default_value(var)}"
                meta_parameter = self.meta_parameter(var)
                if meta_parameter is not None:
                    string += f" [{meta_parameter.units()}]"
                    string += f" {meta_parameter.standard_name()}"
        else:
            string += "\n    *empty*"

        string += "\n" + "Masks:"
        if self.masks():
            max_len = len(max(self.masks(), key=len))
            for mask in self.masks():
                string += f"\n    {mask:{max_len+2}}"
                string += string_of_coords(self.coords(self.coord_group(mask)))
                string += f":  {bool(self.default_value(mask))}"
        else:
            string += "\n    *empty*"

        magnitudes = self.magnitudes()
        string += "\n" + "Magnitudes:"
        if magnitudes:

            for key in magnitudes:
                value = self.get(key)
                string += f"\n  {key}: magnitude of ({value.x},{value.y})"

                meta_parameter = self.meta_parameter(key)
                if meta_parameter is not None:
                    string += f" [{meta_parameter.units()}]"
                    string += f" {meta_parameter.standard_name()}"
        else:
            string += "\n    *empty*"
        directions = self.directions()
        string += "\n" + "Directions:"
        if directions:

            for key in directions:
                value = self.get(key)
                string += f"\n  {key}: direction of ({value.x},{value.y})"
                meta_parameter = self.meta_parameter(key)
                if meta_parameter is not None:
                    string += f" [{meta_parameter.units()}]"
                    string += f" {meta_parameter.standard_name()}"
        else:
            string += "\n    *empty*"

        string += "\n" + "-" * 80
        return string


def move_time_and_spatial_to_front(coord_list: list[str]) -> list[str]:
    """Makes sure that the coordinate list starts with 'time', followed by the spatial coords"""
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
