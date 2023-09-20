from geo_skeletons.point_skeleton import PointSkeleton
import numpy as np


def test_init_trivial():
    grid = PointSkeleton(lon=(1, 2), lat=(0, 3))
    assert grid._ds_manager.vars()[0] == "lat"
    assert grid._ds_manager.vars()[1] == "lon"

    np.testing.assert_array_almost_equal(
        grid._ds_manager.vars_dict()["lat"], np.array([0, 3])
    )
    np.testing.assert_array_almost_equal(
        grid._ds_manager.vars_dict()["lon"], np.array([1, 2])
    )
