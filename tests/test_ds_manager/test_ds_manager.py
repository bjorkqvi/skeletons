from geo_skeletons.point_skeleton import PointSkeleton
import numpy as np


def test_init_trivial():
    grid = PointSkeleton(lon=(1, 2), lat=(0, 3))
    assert list(grid.ds().keys())[0] == "lat"
    assert list(grid.ds().keys())[1] == "lon"

    np.testing.assert_array_almost_equal(grid.ds().lat.values, np.array([0, 3]))
    np.testing.assert_array_almost_equal(grid.ds().lon.values, np.array([1, 2]))
