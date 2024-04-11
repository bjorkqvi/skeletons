from geo_parameters.metaparameter import MetaParameter
from typing import Union
from geo_parameters.grid import Lon, Lat, X, Y, Inds
import numpy as np
import dask.array as da

meta_parameters = {"lon": Lon, "lat": Lat, "x": X, "y": Y, "inds": Inds}

SPATIAL_COORDS = ["y", "x", "lat", "lon", "inds"]


class CoordinateManager:
    """Keeps track of coordinates and data variables that are added to classes
    by the decorators."""

    def __init__(self, initial_coords, initial_vars) -> None:
        self._coords = {}
        self._coords["grid"] = []
        self._coords["gridpoint"] = []
        self._coords["initial"] = []

        self._vars = {}
        self._vars["added"] = {}
        self._vars["initial"] = {}

        self._masks = {}
        self._masks["added"] = {}
        self._masks["opposite"] = {}

        self._default_values = {}

        self.magnitudes = {}
        self.directions = {}

        self.meta_coords: dict[str, MetaParameter] = {}
        self.meta_vars: dict[str, MetaParameter] = {}
        self.meta_masks: dict[str, MetaParameter] = {}
        self.meta_magnitudes: dict[str, MetaParameter] = {}
        self.meta_directions: dict[str, MetaParameter] = {}

        self.set_initial_coords(initial_coords)
        self.set_initial_vars(initial_vars)

        # This will be used by decorators to make a deepcopy of the manager for different classes
        self.initial_state = True

    def add_var(
        self, name: str, coords: str, default_value: float, meta: MetaParameter = None
    ) -> str:
        """Add a variable that the Skeleton will use."""
        name, meta = get_name_str_and_meta(name)
        self._vars["added"][name] = coords
        self._default_values[name] = default_value
        self.meta_vars[name] = meta
        return name

    def add_mask(
        self, name: str, coords: str, default_value: int, opposite_name: str
    ) -> tuple[str, str]:
        """Add a mask that the Skeleton will use."""
        name, meta = get_name_str_and_meta(name)
        self._masks["added"][f"{name}_mask"] = coords

        self._default_values[f"{name}_mask"] = default_value
        self.meta_masks[name] = meta

        if opposite_name is not None:
            opposite_name, meta = get_name_str_and_meta(opposite_name)
            self._masks["opposite"][f"{opposite_name}_mask"] = f"{name}_mask"

        return name, opposite_name

    def add_coord(self, name: str, grid_coord: bool) -> str:
        """Add a coordinate that the Skeleton will use.

        grid_coord = True means that the coordinate describes the outer
        dimensions (e.g. x, y)

        grid_coord = False means that the coordinates describes the inner
        dimensions of one grid point (e.g. frequency, direction)

        E.g. time can be either one (outer dimesnion in spectra, but inner
        dimension in time series)
        """
        name, meta = get_name_str_and_meta(name)
        self.meta_coords[name] = meta
        if grid_coord:
            self._coords["grid"].append(name)
        else:
            self._coords["gridpoint"].append(name)

        return name

    def add_magnitude(self, name: str, x: str, y: str) -> str:
        name, meta = get_name_str_and_meta(name)
        self.magnitudes[name] = {"x": x, "y": y}
        self.meta_magnitudes[name] = meta
        return name

    def add_direction(
        self, name: str, x: str, y: str, meta: MetaParameter = None
    ) -> str:
        name, meta = get_name_str_and_meta(name)
        self.directions[name] = {"x": x, "y": y}
        self.meta_directions[name] = meta
        return name

    def set_initial_vars(self, initial_vars: dict) -> None:
        """Set dictionary containing the initial variables of the Skeleton"""
        if not isinstance(initial_vars, dict):
            raise ValueError("initial_vars needs to be a dict of tuples!")
        for var in self.initial_vars():
            del self.meta_vars[var]
        self._vars["initial"] = initial_vars
        for var in initial_vars:
            self.meta_vars[var] = meta_parameters.get(var)

    def set_initial_coords(self, initial_coords: dict) -> None:
        """Set dictionary containing the initial coordinates of the Skeleton"""
        if not isinstance(initial_coords, list):
            raise ValueError("initial_coords needs to be a list of strings!")
        self._coords["initial"] = initial_coords
        for coord in initial_coords:
            self.meta_coords[coord] = meta_parameters.get(coord)

    def initial_vars(self) -> dict:
        return self._vars["initial"]

    def initial_coords(self) -> dict:
        return self._coords["initial"]

    def added_vars(self) -> dict:
        return self._vars["added"]

    def added_masks(self) -> dict:
        return self._masks["added"]

    def opposite_masks(self) -> dict:
        return self._masks["opposite"]

    def added_coords(self, coords: str = "all") -> list[str]:
        """Returns list of coordinates that have been added to the fixed
        Skeleton coords.

        'all': All added coordinates
        'grid': coordinates for the grid (e.g. z, time)
        'gridpoint': coordinates for a grid point (e.g. frequency, direcion or time)
        """
        if coords not in ["all", "grid", "gridpoint"]:
            print("Variable type needs to be 'all', 'grid' or 'gridpoint'.")
            return None

        if coords == "all":
            return self.added_coords("grid") + self.added_coords("gridpoint")
        return self._coords[coords]

    def coords(self, coords: str = "all") -> list[str]:
        """Returns a list of the coordinates.

        'all' [default]: all coordinates in the Dataset
        'spatial': Dataset coordinates from the Skeleton (x, y, lon, lat, inds)
        'grid': coordinates for the grid (e.g. z, time)
        'gridpoint': coordinates for a grid point (e.g. frequency, direcion or time)
        """

        def list_intersection(list1, list2):
            """Uning intersections of sets doesn't necessarily preserve order"""
            list3 = []
            for val in list1:
                if val in list2:
                    list3.append(val)
            return list3

        if coords not in ["all", "spatial", "grid", "gridpoint"]:
            raise ValueError(
                f"Keyword 'coords' needs to be 'all' (default), 'spatial', 'grid' or 'gridpoint', not {coords}."
            )

        if coords == "spatial":
            return move_time_dim_to_front(
                list_intersection(self.coords("all"), SPATIAL_COORDS)
            )

        if coords == "grid":
            return move_time_dim_to_front(
                self.coords("spatial") + self.added_coords("grid")
            )
        if coords == "gridpoint":
            return move_time_dim_to_front(self.added_coords("gridpoint"))

        if coords == "all":
            return move_time_dim_to_front(
                self.initial_coords() + self.added_coords("all")
            )

    def is_settable(self, name: str) -> bool:
        """Check if the variable etc. is allowed to be set (i.e. is not a magnitude, opposite mask etc.)"""
        return (
            self.added_vars().get(name) is not None
            or self.added_masks().get(name) is not None
        )

    def compute_magnitude(self, x, y):
        return (x**2 + y**2) ** 0.5

    def compute_direction(self, x, y, angular: bool = False, dask: bool = True):
        if dask or hasattr(x, "chunks"):
            dirs = da.arctan2(y, x)
        else:
            dirs = np.arctan2(y, x)

        if not angular:
            dirs = 90 - dirs * 180 / np.pi + 180
            if dask or hasattr(x, "chunks"):
                dirs = da.mod(dirs, 360)
            else:
                dirs = np.mod(dirs, 360)
        return dirs


def move_time_dim_to_front(coord_list) -> list[str]:
    if "time" not in coord_list:
        return coord_list
    coord_list.insert(0, coord_list.pop(coord_list.index("time")))
    return coord_list


def get_name_str_and_meta(
    name: Union[str, MetaParameter]
) -> tuple[str, Union[MetaParameter, None]]:
    """Takes in a string, MetaParameter class or an instamce.
    Returns a string valued name and an instance (if possilbe, otherwise None)"""
    if isinstance(name, str):
        return name, None

    try:
        name = name()
    except TypeError:
        pass
    name_str = name.name  # 'hsig'
    meta = name  # Hs('hsig')

    return name_str, meta
