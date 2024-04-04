from geo_skeletons import GriddedSkeleton, PointSkeleton
from geo_skeletons.decorators import add_datavar, add_magnitude
import numpy as np


def test_magnitude_point():

    @add_magnitude(name="wind", x="u", y="v", direction="wdir")
    @add_datavar("v", default_value=1)
    @add_datavar("u", default_value=1)
    class Magnitude(PointSkeleton):
        pass

    points = Magnitude(x=(0, 1, 2), y=(5, 6, 7))
    points.deactivate_dask()

    assert points.u() is None
    assert points.v() is None
    assert points.wind() is None
    assert points.wdir() is None

    wind = (points.u(empty=True) ** 2 + points.v(empty=True) ** 2) ** 0.5
    np.testing.assert_almost_equal(points.wind(empty=True), wind)

    np.testing.assert_almost_equal(np.median(points.wdir(empty=True)), 45)
    np.testing.assert_almost_equal(
        np.median(points.wdir(empty=True, angular=True)), np.pi / 4
    )

    points.set_u(-1)
    points.set_v(1)
    np.testing.assert_almost_equal(points.wind(), wind)
    np.testing.assert_almost_equal(np.median(points.wdir()), -45 + 360)
    np.testing.assert_almost_equal(np.median(points.wdir(angular=True)), np.pi * 3 / 4)

    points.set_u(2**0.5)
    points.set_v(0)
    np.testing.assert_almost_equal(points.wind(), wind)
    np.testing.assert_almost_equal(np.median(points.wdir()), 90)
    np.testing.assert_almost_equal(np.median(points.wdir(angular=True)), 0)

    points.set_u(-(2**0.5))
    points.set_v(0)
    np.testing.assert_almost_equal(points.wind(), wind)
    np.testing.assert_almost_equal(np.median(points.wdir()), 270)
    np.testing.assert_almost_equal(np.median(points.wdir(angular=True)), np.pi)

    points.set_u(3)
    points.set_v(4)
    np.testing.assert_almost_equal(points.wind(), np.full(points.shape("u"), 5))

    points.set_u(4)
    points.set_v(3)
    np.testing.assert_almost_equal(points.wind(), np.full(points.shape("u"), 5))

    points.set_u(0)
    points.set_v(1)
    np.testing.assert_almost_equal(np.median(points.wdir()), 0)
    np.testing.assert_almost_equal(np.median(points.wdir(angular=True)), np.pi / 2)

    points.set_u(0)
    points.set_v(-1)
    np.testing.assert_almost_equal(np.median(points.wdir()), 180)
    np.testing.assert_almost_equal(np.median(points.wdir(angular=True)), -np.pi / 2)


def test_magnitude_gridded():

    @add_magnitude(name="wind", x="u", y="v", direction="wdir")
    @add_datavar("v", default_value=1)
    @add_datavar("u", default_value=1)
    class Magnitude(GriddedSkeleton):
        pass

    points = Magnitude(x=(0, 1, 2), y=(5, 6, 7, 8))
    points.deactivate_dask()

    assert points.u() is None
    assert points.v() is None
    assert points.wind() is None
    assert points.wdir() is None

    wind = (points.u(empty=True) ** 2 + points.v(empty=True) ** 2) ** 0.5
    np.testing.assert_almost_equal(points.wind(empty=True), wind)

    np.testing.assert_almost_equal(np.median(points.wdir(empty=True)), 45)
    np.testing.assert_almost_equal(
        np.median(points.wdir(empty=True, angular=True)), np.pi / 4
    )

    points.set_u(-1)
    points.set_v(1)
    np.testing.assert_almost_equal(points.wind(), wind)
    np.testing.assert_almost_equal(np.median(points.wdir()), -45 + 360)
    np.testing.assert_almost_equal(np.median(points.wdir(angular=True)), np.pi * 3 / 4)

    points.set_u(2**0.5)
    points.set_v(0)
    np.testing.assert_almost_equal(points.wind(), wind)
    np.testing.assert_almost_equal(np.median(points.wdir()), 90)
    np.testing.assert_almost_equal(np.median(points.wdir(angular=True)), 0)

    points.set_u(-(2**0.5))
    points.set_v(0)
    np.testing.assert_almost_equal(points.wind(), wind)
    np.testing.assert_almost_equal(np.median(points.wdir()), 270)
    np.testing.assert_almost_equal(np.median(points.wdir(angular=True)), np.pi)

    points.set_u(3)
    points.set_v(4)
    np.testing.assert_almost_equal(points.wind(), np.full(points.shape("u"), 5))

    points.set_u(4)
    points.set_v(3)
    np.testing.assert_almost_equal(points.wind(), np.full(points.shape("u"), 5))

    points.set_u(0)
    points.set_v(1)
    np.testing.assert_almost_equal(np.median(points.wdir()), 0)
    np.testing.assert_almost_equal(np.median(points.wdir(angular=True)), np.pi / 2)

    points.set_u(0)
    points.set_v(-1)
    np.testing.assert_almost_equal(np.median(points.wdir()), 180)
    np.testing.assert_almost_equal(np.median(points.wdir(angular=True)), -np.pi / 2)


def test_add_magnitude():

    @add_datavar("v", default_value=1)
    @add_datavar("u", default_value=1)
    class Magnitude(GriddedSkeleton):
        pass

    points = Magnitude(x=(0, 1, 2), y=(5, 6, 7, 8))
    points.add_magnitude("wind", x="u", y="v", direction="wdir")
    points.deactivate_dask()

    assert points.u() is None
    assert points.v() is None
    assert points.wind() is None
    assert points.wdir() is None

    wind = (points.u(empty=True) ** 2 + points.v(empty=True) ** 2) ** 0.5
    np.testing.assert_almost_equal(points.wind(empty=True), wind)

    np.testing.assert_almost_equal(np.median(points.wdir(empty=True)), 45)
    np.testing.assert_almost_equal(
        np.median(points.wdir(empty=True, angular=True)), np.pi / 4
    )

    points.set_u(-1)
    points.set_v(1)
    np.testing.assert_almost_equal(points.wind(), wind)
    np.testing.assert_almost_equal(np.median(points.wdir()), -45 + 360)
    np.testing.assert_almost_equal(np.median(points.wdir(angular=True)), np.pi * 3 / 4)

    points.set_u(2**0.5)
    points.set_v(0)
    np.testing.assert_almost_equal(points.wind(), wind)
    np.testing.assert_almost_equal(np.median(points.wdir()), 90)
    np.testing.assert_almost_equal(np.median(points.wdir(angular=True)), 0)

    points.set_u(-(2**0.5))
    points.set_v(0)
    np.testing.assert_almost_equal(points.wind(), wind)
    np.testing.assert_almost_equal(np.median(points.wdir()), 270)
    np.testing.assert_almost_equal(np.median(points.wdir(angular=True)), np.pi)

    points.set_u(3)
    points.set_v(4)
    np.testing.assert_almost_equal(points.wind(), np.full(points.shape("u"), 5))

    points.set_u(4)
    points.set_v(3)
    np.testing.assert_almost_equal(points.wind(), np.full(points.shape("u"), 5))

    points.set_u(0)
    points.set_v(1)
    np.testing.assert_almost_equal(np.median(points.wdir()), 0)
    np.testing.assert_almost_equal(np.median(points.wdir(angular=True)), np.pi / 2)

    points.set_u(0)
    points.set_v(-1)
    np.testing.assert_almost_equal(np.median(points.wdir()), 180)
    np.testing.assert_almost_equal(np.median(points.wdir(angular=True)), -np.pi / 2)
