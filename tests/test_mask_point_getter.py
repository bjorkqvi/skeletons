from geo_skeletons import GriddedSkeleton, PointSkeleton
from geo_skeletons.decorators import add_mask
import numpy as np


def test_trivial_mask():
    @add_mask("land", opposite_name="sea")
    class Dummy(PointSkeleton):
        pass

    points = Dummy(lon=(1, 2, 3, 4), lat=(6, 7, 8, 9))
    points.set_land_mask(True)

    lon, lat = points.land_points()
    np.testing.assert_array_almost_equal(points.lon(), lon)
    np.testing.assert_array_almost_equal(points.lat(), lat)

    lon, lat = points.get("land_points")
    np.testing.assert_array_almost_equal(points.lon(), lon)
    np.testing.assert_array_almost_equal(points.lat(), lat)

    lon, lat = points.sea_points()
    assert not np.any(lon)
    assert not np.any(lat)

    lon, lat = points.get("sea_points")
    assert not np.any(lon)
    assert not np.any(lat)
