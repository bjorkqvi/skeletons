from geo_skeletons.point_skeleton import PointSkeleton
from geo_skeletons.decorators import add_coord, add_datavar

import numpy as np


def test_add_datavar():
    @add_datavar(name="hs", default_value=0.0)
    class WaveHeight(PointSkeleton):
        pass

    data = WaveHeight(lon=(10, 20), lat=(30, 40))
    np.testing.assert_almost_equal(np.mean(data.hs()), 0.0)
    assert data.hs(strict=True) is None
    data.set_hs(1)
    assert np.mean(data.hs()) == 1
    data.set_hs(1.0)
    assert np.mean(data.hs()) == 1.0
    data.set_hs(np.full(data.size(), 2.0))
    assert np.mean(data.hs()) == 2.0


def test_add_coord_and_datavar():
    @add_datavar(name="hs", default_value=0.0)
    @add_coord(name="z", grid_coord=True)
    class WaveHeight(PointSkeleton):
        pass

    data = WaveHeight(lon=(10, 20), lat=(30, 40), z=(1, 2, 3))
    np.testing.assert_almost_equal(np.mean(data.hs()), 0.0)
    assert data.hs(strict=True) is None
    data.set_hs(1)
    assert np.mean(data.hs()) == 1
    data.set_hs(1.0)
    assert np.mean(data.hs()) == 1.0
    data.set_hs(np.full(data.size(), 2.0))
    assert np.mean(data.hs()) == 2.0
