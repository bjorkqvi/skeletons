from geo_skeletons import GriddedSkeleton
from geo_skeletons.decorators import add_coord, add_datavar, add_time
import numpy as np
import pandas as pd
import pytest


def test_iter_over_points():
    """Iterates over points"""
    y = (5, 6, 7, 8, 9)
    x = (0, 1, 2)
    iter_y = [5, 6, 7, 8, 9, 5, 6, 7, 8, 9, 5, 6, 7, 8, 9]
    iter_x = [0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2]
    points = GriddedSkeleton(x=x, y=y)
    for n, point in enumerate(points):  # Iterates over y first
        assert isinstance(point, GriddedSkeleton)
        assert point.size() == (1, 1)
        assert point.y()[0] == iter_y[n]
        assert point.x()[0] == iter_x[n]
    assert n == len(x) * len(y) - 1


def test_iter_over_points_flip_xy():
    """Iterates over points"""
    y = (5, 6, 7, 8, 9)
    x = (0, 1, 2)
    iter_x = [0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2]
    iter_y = [5, 5, 5, 6, 6, 6, 7, 7, 7, 8, 8, 8, 9, 9, 9]
    points = GriddedSkeleton(x=x, y=y)
    for n, point in enumerate(points.iterate(["x", "y"])):  # Iterates over x first
        assert isinstance(point, GriddedSkeleton)
        assert point.size() == (1, 1)
        assert point.y()[0] == iter_y[n]
        assert point.x()[0] == iter_x[n]
    assert n == len(x) * len(y) - 1


def test_iter_over_points_xslices():
    """Iterates over points"""
    y = (5, 6, 7, 8, 9)
    x = (0, 1, 2, 3)
    points = GriddedSkeleton(x=x, y=y)
    for n, point in enumerate(
        points.iterate(["x"])
    ):  # Iterates over x only to get y-slices
        assert isinstance(point, GriddedSkeleton)
        assert point.size() == (len(y), 1)
        assert point.x()[0] == x[n]
        np.testing.assert_almost_equal(point.y(), np.array(y))
    assert n == len(x) - 1


def test_iter_over_points_added_gridpoint_coord():
    """Iterates over points and not over added gridpoint coordinate"""

    # Not grid-coordinate and not iterated over by default
    @add_coord(name="z")
    class Expanded(GriddedSkeleton):
        pass

    y = (5, 6)
    x = (0, 1, 2)
    z = (10, 11, 12, 13)
    iter_y = [5, 6, 5, 6, 5, 6]
    iter_x = [0, 0, 1, 1, 2, 2]
    points = Expanded(x=x, y=y, z=z)
    for n, point in enumerate(points):
        assert isinstance(point, Expanded)
        assert point.size() == (1, 1, len(z))
        assert point.y()[0] == iter_y[n]
        assert point.x()[0] == iter_x[n]
        np.testing.assert_almost_equal(points.z(), z)
    assert n == len(x) * len(y) - 1


def test_iter_over_points_added_grid_coord():
    """Iterates over points and added grid coordinate"""

    # Should be iterated over by default
    @add_coord(name="z", grid_coord=True)
    class Expanded(GriddedSkeleton):
        pass

    y = (5, 6)
    x = (0, 1, 2)
    z = (10, 11, 12, 13)
    iter_y = [5, 6, 5, 6, 5, 6, 5, 6, 5, 6, 5, 6, 5, 6, 5, 6, 5, 6, 5, 6, 5, 6, 5, 6]
    iter_x = [0, 0, 1, 1, 2, 2, 0, 0, 1, 1, 2, 2, 0, 0, 1, 1, 2, 2, 0, 0, 1, 1, 2, 2]
    iter_z = [
        10,
        10,
        10,
        10,
        10,
        10,
        11,
        11,
        11,
        11,
        11,
        11,
        12,
        12,
        12,
        12,
        12,
        12,
        13,
        13,
        13,
        13,
        13,
        13,
    ]
    points = Expanded(x=x, y=y, z=z)
    for n, point in enumerate(points):  # ['y','x','z']
        assert isinstance(point, Expanded)
        assert point.size() == (1, 1, 1)
        assert point.y()[0] == iter_y[n]
        assert point.x()[0] == iter_x[n]
        assert point.z()[0] == iter_z[n]
    assert n == len(x) * len(y) * len(z) - 1


def test_iter_over_points_added_gridpoint_coord_included():
    """Iterates over points and added grid coordinate"""

    # Should NOT be iterated over by default
    @add_coord(name="z")
    class Expanded(GriddedSkeleton):
        pass

    y = (5, 6)
    x = (0, 1, 2)
    z = (10, 11, 12, 13)
    iter_y = [5, 6, 5, 6, 5, 6, 5, 6]
    iter_z = [10, 10, 11, 11, 12, 12, 13, 13]
    points = Expanded(x=x, y=y, z=z)
    for n, point in enumerate(points.iterate(["y", "z"])):
        assert isinstance(point, Expanded)
        assert point.size() == (1, len(x), 1)
        assert point.y()[0] == iter_y[n]
        np.testing.assert_almost_equal(points.x(), x)
        assert point.z()[0] == iter_z[n]

    assert n == len(y) * len(z) - 1


def test_iter_over_points_not_time():
    """Iterates over points and not added time coordinate"""

    # Should NOT be iterated over by default
    @add_time()
    class Expanded(GriddedSkeleton):
        pass

    y = (5, 6, 7, 8, 9)
    x = (0, 1, 2)
    iter_y = [5, 6, 7, 8, 9, 5, 6, 7, 8, 9, 5, 6, 7, 8, 9]
    iter_x = [0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2]
    time = pd.date_range("2020-01-01 00:00", "2020-01-01 23:00", freq="h")
    points = Expanded(x=x, y=y, time=time)
    for n, point in enumerate(points):
        assert isinstance(point, Expanded)
        assert point.size() == (24, 1, 1)
        assert point.y()[0] == iter_y[n]
        assert point.x()[0] == iter_x[n]
        assert (
            point.time(datetime=False, fmt="%Y-%m-%d %H:%M")
            == time.strftime("%Y-%m-%d %H:%M").to_list()
        )

    assert n == (len(x) * len(y) - 1)


def test_iter_over_points_and_time():
    """Iterates over points and added time coordinate"""

    @add_time(grid_coord=True)
    class Expanded(GriddedSkeleton):
        pass

    y = (5, 6)
    x = (0, 1, 2)
    time = pd.date_range("2020-01-01 00:00", "2020-01-01 01:00", freq="h")

    iter_y = [5, 5, 6, 6, 5, 5, 6, 6, 5, 5, 6, 6]
    iter_x = [0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2]
    iter_time = [
        "2020-01-01 00:00",
        "2020-01-01 01:00",
        "2020-01-01 00:00",
        "2020-01-01 01:00",
        "2020-01-01 00:00",
        "2020-01-01 01:00",
        "2020-01-01 00:00",
        "2020-01-01 01:00",
        "2020-01-01 00:00",
        "2020-01-01 01:00",
        "2020-01-01 00:00",
        "2020-01-01 01:00",
    ]

    points = Expanded(x=x, y=y, time=time)
    for n, point in enumerate(points):  # Iterates first over time
        assert isinstance(point, Expanded)
        assert point.size() == (1, 1, 1)
        assert point.y()[0] == iter_y[n]
        assert point.x()[0] == iter_x[n]
        assert point.time(datetime=False, fmt="%Y-%m-%d %H:%M") == [iter_time[n]]

    assert n == len(x) * len(y) * len(time) - 1


def test_iter_over_points_and_time_time_last():
    """Iterates over points and added time coordinate"""

    @add_time(grid_coord=True)
    class Expanded(GriddedSkeleton):
        pass

    y = (5, 6)
    x = (0, 1, 2)
    time = pd.date_range("2020-01-01 00:00", "2020-01-01 01:00", freq="h")

    iter_y = [5, 6, 5, 6, 5, 6, 5, 6, 5, 6, 5, 6]
    iter_x = [0, 0, 1, 1, 2, 2, 0, 0, 1, 1, 2, 2]
    iter_time = [
        "2020-01-01 00:00",
        "2020-01-01 00:00",
        "2020-01-01 00:00",
        "2020-01-01 00:00",
        "2020-01-01 00:00",
        "2020-01-01 00:00",
        "2020-01-01 01:00",
        "2020-01-01 01:00",
        "2020-01-01 01:00",
        "2020-01-01 01:00",
        "2020-01-01 01:00",
        "2020-01-01 01:00",
    ]
    points = Expanded(x=x, y=y, time=time)
    for n, point in enumerate(
        points.iterate(["y", "x", "time"])
    ):  # Iterates last over time
        assert isinstance(point, Expanded)
        assert point.size() == (1, 1, 1)
        assert point.y()[0] == iter_y[n]
        assert point.x()[0] == iter_x[n]
        assert point.time(datetime=False, fmt="%Y-%m-%d %H:%M") == [iter_time[n]]
    assert n == len(x) * len(y) * len(time) - 1


def test_iter_over_wrong_coord_raises_error():
    """Iterates over points and added time coordinate"""

    @add_time(grid_coord=True)
    class Expanded(GriddedSkeleton):
        pass

    y = (5, 6)
    x = (0, 1, 2)
    time = pd.date_range("2020-01-01 00:00", "2020-01-01 01:00", freq="h")
    points = Expanded(x=x, y=y, time=time)
    with pytest.raises(KeyError):
        for point in points.iterate(
            ["y", "x", "tmie"]
        ):  # Wrong name of dimension given
            pass
