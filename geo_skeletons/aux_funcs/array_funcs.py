import pandas as pd
import numpy as np
from typing import Iterable


def coord_len_to_max_two(xvec):
    if xvec is not None and len(xvec) > 2:
        xvec = np.array([min(xvec), max(xvec)])
    return xvec


def sanitize_singe_variable(name: str, x):
    """Forces to nump array and checks dimensions etc"""
    x = force_to_iterable(x)

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
    if not isinstance(time, Iterable):
        return pd.DatetimeIndex([time])
    return pd.DatetimeIndex(time)


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


def force_to_iterable(x, fmt: str = None) -> Iterable:
    """Returns an numpy array with at least one dimension and Nones removed

    Will return None if given None."""
    if x is None:
        return None

    x = np.atleast_1d(x)
    x = np.array([a for a in x if a is not None])

    return x


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
