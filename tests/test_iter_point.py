from geo_skeletons import PointSkeleton
from geo_skeletons.decorators import add_coord, add_datavar
import numpy as np


def test_iter_over_points():
    """Iterates over points"""
    y = (5, 6, 7, 8)
    x = (0, 1, 2, 3)
    points = PointSkeleton(x=x, y=y)
    for n, point in enumerate(points):
        assert isinstance(point, PointSkeleton)
        assert point.size() == (1,)
        assert point.inds()[0] == 0
        assert point.y()[0] == y[n]
        assert point.x()[0] == x[n]
    assert n == len(x) - 1


def test_iter_over_points_added_gridpoint_coord():
    """Iterates over points and not over added gridpoint coordinate"""

    # Not grid-coordinate and not iterated over by default
    @add_coord(name="z")
    class Expanded(PointSkeleton):
        pass

    y = (5, 6, 7, 8)
    x = (0, 1, 2, 3)
    z = (10, 11, 12)
    points = Expanded(x=x, y=y, z=z)
    for n, point in enumerate(points):
        assert isinstance(point, Expanded)
        assert point.size() == (1, len(z))
        assert point.inds()[0] == 0
        assert point.y()[0] == y[n]
        assert point.x()[0] == x[n]
        np.testing.assert_almost_equal(points.z(), z)
    assert n == len(x) - 1


def test_iter_over_points_added_grid_coord():
    """Iterates over points and added grid coordinate"""

    # Should be iterated over by default
    @add_coord(name="z", grid_coord=True)
    class Expanded(PointSkeleton):
        pass

    y = (5, 6, 7, 8)
    x = (0, 1, 2, 3)
    z = (10, 11, 12)
    points = Expanded(x=x, y=y, z=z)
    for n, point in enumerate(points):
        assert isinstance(point, Expanded)
        assert point.size() == (1, 1)
        assert point.inds()[0] == 0
        assert point.y()[0] == y[n % len(y)]
        assert point.x()[0] == x[n % len(x)]
        assert point.z()[0] == z[int(np.floor(n / len(y)))]

    assert n == len(x) * len(z) - 1


def test_iter_over_points_added_gridpoint_coord_included():
    """Iterates over points and added grid coordinate"""

    # Should NOT be iterated over by default
    @add_coord(name="z")
    class Expanded(PointSkeleton):
        pass

    y = (5, 6, 7, 8)
    x = (0, 1, 2, 3)
    z = (10, 11, 12)
    points = Expanded(x=x, y=y, z=z)
    for n, point in enumerate(iter(points)(["inds", "z"])):
        assert isinstance(point, Expanded)
        assert point.size() == (1, 1)
        assert point.inds()[0] == 0
        assert point.y()[0] == y[n % len(y)]
        assert point.x()[0] == x[n % len(x)]
        assert point.z()[0] == z[int(np.floor(n / len(y)))]

    assert n == len(x) * len(z) - 1
