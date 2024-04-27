from .coordinate_manager import (
    CoordinateManager,
    move_time_and_spatial_to_front,
    SPATIAL_COORDS,
)

import pandas as pd
import numpy as np
from typing import Iterable

from ..errors import (
    DataWrongDimensionError,
    UnknownCoordinateError,
    CoordinateWrongLengthError,
    GridError,
)


class DataSanitizer:
    def __init__(self, coord_manager: CoordinateManager) -> None:
        self.coord_manager = coord_manager

    @staticmethod
    def sanitize_input(x, y, lon, lat, is_gridded_format, **kwargs):
        """Sanitizes input. After this all variables are either
        non-empty np.ndarrays with len >= 1 or None"""

        spatial = {"x": x, "y": y, "lon": lon, "lat": lat}
        for key, value in spatial.items():
            spatial[key] = sanitize_singe_variable(key, value)

        other = {}
        for key, value in kwargs.items():
            if key == "time":
                # other[key] = sanitize_singe_variable(key, value, fmt="datetime")
                other[key] = sanitize_time_input(value)
            else:
                other[key] = sanitize_singe_variable(key, value)

        if is_gridded_format:
            spatial = get_unique_values(spatial)

        else:
            spatial = sanitize_point_structure(spatial)

            for x, y in [("x", "y"), ("lon", "lat")]:
                length_ok = check_that_variables_equal_length(spatial[x], spatial[y])
                if not length_ok:
                    raise Exception(
                        f"{x} is length {len(spatial[x])} but {y} is length {len(spatial[y])}!"
                    )

        if np.all([a is None for a in spatial.values()]):
            raise Exception("x, y, lon, lat cannot ALL be None!")

        if spatial["lon"] is not None:
            spatial["lon"] = clean_lons(spatial["lon"])

        return spatial["x"], spatial["y"], spatial["lon"], spatial["lat"], other

    @staticmethod
    def force_to_iterable(x, fmt: str = None) -> Iterable:
        """Returns an numpy array with at least one dimension and Nones removed

        Will return None if given None."""
        if x is None:
            return None

        x = np.atleast_1d(x)
        x = np.array([a for a in x if a is not None])

        return x

    def check_consistency(self, coord_dict, var_dict) -> None:
        """Checks that the provided coordinates are consistent with the
        coordinates that the Skeleton is defined over."""
        coords = list(coord_dict.keys())
        # Check spatial coordinates
        xy_set = "x" in coords and "y" in coords
        lonlat_set = "lon" in coords and "lat" in coords
        inds_set = "inds" in coords
        if inds_set:
            ind_len = len(coord_dict["inds"])
            for key, value in var_dict.items():
                if len(value[1]) != ind_len:
                    raise CoordinateWrongLengthError(
                        variable=key,
                        len_of_variable=len(value[1]),
                        index_variable="inds",
                        len_of_index_variable=ind_len,
                    )
        if not (xy_set or lonlat_set or inds_set):
            raise GridError
        if sum([xy_set, lonlat_set, inds_set]) > 1:
            raise GridError

        # Check that all added coordinates are provided
        for coord in self.coord_manager.coords("all"):
            if coord not in coords:
                if self.get(coord) is not None:
                    # Add in old coordinate if it is not provided now (can happen when using set_spacing)
                    coord_dict[coord] = self.ds().get(coord).values
                else:
                    raise UnknownCoordinateError(
                        f"Skeleton has coordinate '{coord}', but it was not provided on initialization: {coords}!"
                    )

        # Check that all provided coordinates have been added
        for coord in set(coords) - set(SPATIAL_COORDS):
            if coord not in self.coord_manager.coords("all"):
                raise UnknownCoordinateError(
                    f"Coordinate {coord} provided on initialization, but Skeleton doesn't have it: {self.coord_manager.coords('all')}! Missing a decorator?"
                )


def will_grid_be_spherical_or_cartesian(x, y, lon, lat):
    """Determines if the grid will be spherical or cartesian based on which
    inputs are given and which are None.

    Returns the ringth vector and string to identify the native values.
    """

    # Check for empty grid
    if (
        (lon is None or len(lon) == 0)
        and (lat is None or len(lat) == 0)
        and (x is None or len(x) == 0)
        and (y is None or len(y) == 0)
    ):
        native_x = "x"
        native_y = "y"
        xvec = np.array([])
        yvec = np.array([])
        return native_x, native_y, xvec, yvec

    xy = False
    lonlat = False

    if (x is not None) and (y is not None):
        xy = True
        native_x = "x"
        native_y = "y"
        xvec = x
        yvec = y

    if (lon is not None) and (lat is not None):
        lonlat = True
        native_x = "lon"
        native_y = "lat"
        xvec = lon
        yvec = lat

    if xy and lonlat:
        raise ValueError("Can't set both lon/lat and x/y!")

    # Empty grid will be cartesian
    if not xy and not lonlat:
        native_x = "x"
        native_y = "y"
        xvec = np.array([])
        yvec = np.array([])

    return native_x, native_y, xvec, yvec


def coord_len_to_max_two(xvec):
    if xvec is not None and len(xvec) > 2:
        xvec = np.array([min(xvec), max(xvec)])
    return xvec


def sanitize_singe_variable(name: str, x):
    """Forces to nump array and checks dimensions etc"""
    x = DataSanitizer(None).force_to_iterable(x)

    # np.array([None, None]) -> None
    if x is None or all(v is None for v in x):
        x = None

    if x is not None and len(x.shape) > 1:
        raise Exception(
            f"Vector {name} should have one dimension, but it has dimensions {x.shape}!"
        )

    # Set np.array([]) to None
    if x is not None and x.shape == (0,):
        x = None

    return x


def sanitize_point_structure(spatial: dict) -> dict:
    """Repeats a single value to match lenths of arrays"""
    x = spatial.get("x")
    y = spatial.get("y")
    lon = spatial.get("lon")
    lat = spatial.get("lat")

    if x is not None and y is not None:
        if len(x) != len(y):
            if len(x) == 1:
                spatial["x"] = np.repeat(x[0], len(y))
            elif len(y) == 1:
                spatial["y"] = np.repeat(y[0], len(x))
            else:
                raise Exception(
                    f"x-vector is {len(x)} long but y-vecor is {len(y)} long!"
                )
    if lon is not None and lat is not None:
        if len(lon) != len(lat):
            if len(lon) == 1:
                spatial["lon"] = np.repeat(lon[0], len(lat))
            elif len(lat) == 1:
                spatial["lat"] = np.repeat(lat[0], len(lon))
            else:
                raise Exception(
                    f"x-vector is {len(lon)} long but y-vecor is {len(lat)} long!"
                )

    return spatial


def get_edges_of_arrays(spatial: dict) -> dict:
    """Takes only edges of arrays, so [1,2,3] -> [1,3]"""
    for key, value in spatial.items():
        if value is not None:
            spatial[key] = coord_len_to_max_two(value)

    return spatial


def check_that_variables_equal_length(x, y) -> bool:
    if x is None and y is None:
        return True
    if x is None:
        raise ValueError(f"x/lon variable None even though y/lat variable is not!")
    if y is None:
        raise ValueError(f"y/lat variable None even though x/lon variable is not!")
    return len(x) == len(y)


def sanitize_time_input(time):
    if isinstance(time, str):
        return pd.DatetimeIndex([time])
    if isinstance(time, np.ndarray):
        return pd.DatetimeIndex(np.atleast_1d(time))
    if not isinstance(time, Iterable):
        return pd.DatetimeIndex([time])
    return pd.DatetimeIndex(time)


def clean_lons(lon):
    mask = lon < -180
    lon[mask] = lon[mask] + 360
    mask = lon > 180
    lon[mask] = lon[mask] - 360
    return lon


def get_unique_values(spatial):
    """e.g. lon=(4.0, 4.0) should behave like lon=4.0 if data is gridded"""
    if spatial.get("lon") is not None and spatial.get("lat") is not None:
        coords = ["lon", "lat"]
    elif spatial.get("x") is not None and spatial.get("y") is not None:
        coords = ["x", "y"]

    for coord in coords:
        val = spatial.get(coord)
        if len(np.unique(val)) == 1 and len(val) == 2:
            spatial[coord] = np.unique(val)
    return spatial
