from geo_skeletons.errors import (
    GridError,
    CoordinateWrongLengthError,
    CoordinateWrongDimensionError,
)

import pytest
import numpy as np
from geo_skeletons.data_sanitizer import data_sanitizer
import pandas as pd


def test_sanitize_input_x_y():
    x, y, lon, lat, other = data_sanitizer.sanitize_input(
        x=0, y=1, lon=None, lat=None, is_gridded_format=False
    )
    assert isinstance(x, np.ndarray)
    assert isinstance(y, np.ndarray)
    assert lon is None
    assert lat is None

    assert x.shape == (1,)
    assert y.shape == (1,)


def test_sanitize_input_x_y_gridded():
    x, y, lon, lat, other = data_sanitizer.sanitize_input(
        x=0, y=1, lon=None, lat=None, is_gridded_format=True
    )
    assert isinstance(x, np.ndarray)
    assert isinstance(y, np.ndarray)
    assert lon is None
    assert lat is None

    assert x.shape == (1,)
    assert y.shape == (1,)


def test_sanitize_input_lon_lat():
    x, y, lon, lat, other = data_sanitizer.sanitize_input(
        x=None, y=None, lon=0, lat=1, is_gridded_format=False
    )
    assert isinstance(lon, np.ndarray)
    assert isinstance(lat, np.ndarray)
    assert x is None
    assert y is None

    assert lon.shape == (1,)
    assert lat.shape == (1,)


def test_sanitize_input_lon_lat_gridded():
    x, y, lon, lat, other = data_sanitizer.sanitize_input(
        x=None, y=None, lon=0, lat=1, is_gridded_format=True
    )
    assert isinstance(lon, np.ndarray)
    assert isinstance(lat, np.ndarray)
    assert x is None
    assert y is None

    assert lon.shape == (1,)
    assert lat.shape == (1,)


def test_sanitize_input_all_none():
    with pytest.raises(GridError):
        data_sanitizer.sanitize_input(None, None, None, None, is_gridded_format=False)
    with pytest.raises(GridError):
        data_sanitizer.sanitize_input(None, None, None, None, is_gridded_format=True)


def test_var_equal_length():
    assert (
        data_sanitizer.check_that_variables_equal_length(
            np.array([1, 2, 3]), np.array([6, 7, 8])
        )
        == True
    )

    with pytest.raises(ValueError):
        data_sanitizer.check_that_variables_equal_length(None, np.array([6, 7, 8]))

    with pytest.raises(ValueError):
        data_sanitizer.check_that_variables_equal_length(np.array([1, 2, 3]), None)

    assert data_sanitizer.check_that_variables_equal_length(None, None) == True

    with pytest.raises(CoordinateWrongLengthError):
        data_sanitizer.check_that_variables_equal_length(
            np.array([1, 2, 3]), np.array([6, 7, 8, 7])
        )


def test_coord_len_to_max_two():
    x = np.array([1, 2, 3])
    x2 = data_sanitizer.coord_len_to_max_two(x)

    np.testing.assert_array_almost_equal(x2, np.array([1, 3]))
    np.testing.assert_array_almost_equal(x2, data_sanitizer.coord_len_to_max_two(x))

    assert data_sanitizer.coord_len_to_max_two(None) is None


def test_sanitize_single_variable():
    assert data_sanitizer.sanitize_singe_variable("test", None) is None
    assert data_sanitizer.sanitize_singe_variable("test", [None, None]) is None
    assert (
        data_sanitizer.sanitize_singe_variable("test", np.array([None, None])) is None
    )

    data_sanitizer.sanitize_singe_variable("test", np.zeros((2, 1))).shape == (2,)

    assert data_sanitizer.sanitize_singe_variable("test", np.array([])) is None


def test_sanitize_point_structure():
    x = np.array([1, 2])
    y = np.array([1, 2, 4])
    with pytest.raises(ValueError):
        data_sanitizer.sanitize_point_structure({"lon": x, "lat": y})
    with pytest.raises(ValueError):
        data_sanitizer.sanitize_point_structure({"x": x, "y": y})

    coords = data_sanitizer.sanitize_point_structure(
        {"lon": np.array([1, 2]), "lat": np.array([3])}
    )

    np.testing.assert_array_almost_equal(coords["lat"], np.array([3, 3]))

    coords = data_sanitizer.sanitize_point_structure(
        {"lat": np.array([1, 2]), "lon": np.array([3])}
    )

    np.testing.assert_array_almost_equal(coords["lon"], np.array([3, 3]))

    coords = data_sanitizer.sanitize_point_structure(
        {"x": np.array([1, 2]), "y": np.array([3])}
    )

    np.testing.assert_array_almost_equal(coords["y"], np.array([3, 3]))

    coords = data_sanitizer.sanitize_point_structure(
        {"y": np.array([1, 2]), "x": np.array([3])}
    )

    np.testing.assert_array_almost_equal(coords["x"], np.array([3, 3]))


def test_grid_spherical_or_cartesian():
    native_x, native_y, xvec, yvec = data_sanitizer.will_grid_be_spherical_or_cartesian(
        None, None, None, None
    )

    assert native_x == "x"
    assert native_y == "y"
    np.testing.assert_array_equal(xvec, np.array([]))
    np.testing.assert_array_equal(yvec, np.array([]))

    with pytest.raises(ValueError):
        data_sanitizer.will_grid_be_spherical_or_cartesian(
            np.array([0]), np.array([0]), np.array([0]), np.array([0])
        )


def test_sanitize_time_input():
    """if isinstance(time, str):
        return pd.DatetimeIndex([time])
    if isinstance(time, np.ndarray):
        return pd.DatetimeIndex(np.atleast_1d(time))
    if not isinstance(time, Iterable):
        return pd.DatetimeIndex([time])
    return pd.DatetimeIndex(time)
    """

    time = "2020-01-04 03:00"
    time2 = data_sanitizer.sanitize_time_input(time)
    assert isinstance(time2, pd.DatetimeIndex)
    assert len(time2) == 1
    assert time2.strftime("%Y%m%d%H%M") == "202001040300"

    time = np.array([time])
    time2 = data_sanitizer.sanitize_time_input(time)
    assert isinstance(time2, pd.DatetimeIndex)
    assert len(time2) == 1
    assert time2.strftime("%Y%m%d%H%M") == "202001040300"

    time = pd.to_datetime("2020-01-04 03:00")
    time2 = data_sanitizer.sanitize_time_input(time)
    assert isinstance(time2, pd.DatetimeIndex)
    assert len(time2) == 1
    assert time2.strftime("%Y%m%d%H%M") == "202001040300"

    time = pd.date_range("2020-01-04 03:00", "2020-01-05 06:00", freq="1h")
    time2 = data_sanitizer.sanitize_time_input(time)
    assert isinstance(time2, pd.DatetimeIndex)
    assert len(time2) == 28
    assert time2[0].strftime("%Y%m%d%H%M") == "202001040300"
    assert time2[-1].strftime("%Y%m%d%H%M") == "202001050600"
