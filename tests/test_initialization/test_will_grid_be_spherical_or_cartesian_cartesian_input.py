from geo_skeletons.data_sanitizer import sanitize_input
from geo_skeletons.data_sanitizer import (
    will_grid_be_spherical_or_cartesian as func,
)
import numpy as np


def test_x_y_tuple():
    lon, lat = None, None
    x, y = (0.0, 1.0), (2.0, 3.0)

    for is_gridded in [True, False]:
        x, y, lon, lat, __ = sanitize_input(x, y, lon, lat, is_gridded)

        native_x, native_y, xvec, yvec = func(lon=lon, lat=lat, x=x, y=y)
        assert native_x == "x"
        assert native_y == "y"
        assert np.all(xvec == np.array([0.0, 1.0]))
        assert np.all(yvec == np.array([2.0, 3.0]))


def test_x_y_tuple_none_tuple():
    lon, lat = (None, None), (None, None)
    x, y = (0.0, 1.0), (2.0, 3.0)

    for is_gridded in [True, False]:
        x, y, lon, lat, __ = sanitize_input(x, y, lon, lat, is_gridded)

        native_x, native_y, xvec, yvec = func(lon=lon, lat=lat, x=x, y=y)
        assert native_x == "x"
        assert native_y == "y"
        assert np.all(xvec == np.array([0.0, 1.0]))
        assert np.all(yvec == np.array([2.0, 3.0]))


def test_x_y_int_tuple():
    lon, lat = None, None
    x, y = (0, 1), (2, 3)
    for is_gridded in [True, False]:
        x, y, lon, lat, __ = sanitize_input(x, y, lon, lat, is_gridded)

        native_x, native_y, xvec, yvec = func(lon=lon, lat=lat, x=x, y=y)
        assert native_x == "x"
        assert native_y == "y"
        assert np.all(xvec == np.array([0.0, 1.0]))
        assert np.all(yvec == np.array([2.0, 3.0]))


def test_x_lat_single_tuple():
    lon, lat = None, None
    x, y = (0.0), (2.0)

    for is_gridded in [True, False]:
        x, y, lon, lat, __ = sanitize_input(x, y, lon, lat, is_gridded)

        native_x, native_y, xvec, yvec = func(lon=lon, lat=lat, x=x, y=y)
        assert native_x == "x"
        assert native_y == "y"
        assert np.all(xvec == np.array([0.0]))
        assert np.all(yvec == np.array([2.0]))


def test_x_y_single_value():
    lon, lat = None, None
    x, y = 0.0, 2.0

    for is_gridded in [True, False]:
        x, y, lon, lat, __ = sanitize_input(x, y, lon, lat, is_gridded)

        native_x, native_y, xvec, yvec = func(lon=lon, lat=lat, x=x, y=y)
        assert native_x == "x"
        assert native_y == "y"
        assert np.all(xvec == np.array([0.0]))
        assert np.all(yvec == np.array([2.0]))


def test_x_y_array():
    lon, lat = None, None
    x, y = np.array([0.0, 1.0, 2.0, 3.0]), np.array([2.0, 3.0, 4.0, 5.0])

    for is_gridded in [True, False]:
        x, y, lon, lat, __ = sanitize_input(x, y, lon, lat, is_gridded)

        native_x, native_y, xvec, yvec = func(lon=lon, lat=lat, x=x, y=y)
        assert native_x == "x"
        assert native_y == "y"
        assert np.all(xvec == np.array([0.0, 1.0, 2.0, 3.0]))
        assert np.all(yvec == np.array([2.0, 3.0, 4.0, 5.0]))


def test_x_y_int_array():
    lon, lat = None, None
    x, y = np.array([0, 1, 2, 3]), np.array([2, 3, 4, 5])
    for is_gridded in [True, False]:
        x, y, lon, lat, __ = sanitize_input(x, y, lon, lat, is_gridded)

        native_x, native_y, xvec, yvec = func(lon=lon, lat=lat, x=x, y=y)
        assert native_x == "x"
        assert native_y == "y"
        assert np.all(xvec == np.array([0, 1, 2, 3]))
        assert np.all(yvec == np.array([2, 3, 4, 5]))
