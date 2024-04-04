from geo_skeletons import GriddedSkeleton, PointSkeleton
from geo_skeletons.decorators import add_datavar, add_magnitude
import numpy as np


def test_magnitude():

    @add_magnitude(name="wind", x="u", y="v")
    @add_datavar("v")
    @add_datavar("u")
    class Magnitude(PointSkeleton):
        pass

    points = Magnitude(x=(0, 1, 2), y=(5, 6, 7))
    points.set_u(2)
    points.set_v(1)
    wind = (points.u(dask=False) ** 2 + points.v(dask=False) ** 2) ** 0.5
    np.testing.assert_almost_equal(points.wind(dask=False), wind)
