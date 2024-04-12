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
    np.testing.assert_almost_equal(np.median(points.wdir(empty=True)), 45 + 180)
    np.testing.assert_almost_equal(
        np.median(points.wdir(empty=True, angular=True)), np.pi / 4
    )

    points.set_u(-1)
    points.set_v(1)
    np.testing.assert_almost_equal(points.wind(), wind)
    np.testing.assert_almost_equal(np.median(points.wdir()), -45 + 360 - 180)
    np.testing.assert_almost_equal(np.median(points.wdir(angular=True)), np.pi * 3 / 4)

    points.set_u(2**0.5)
    points.set_v(0)
    np.testing.assert_almost_equal(points.wind(), wind)
    np.testing.assert_almost_equal(np.median(points.wdir()), 90 + 180)
    np.testing.assert_almost_equal(np.median(points.wdir(angular=True)), 0)

    points.set_u(-(2**0.5))
    points.set_v(0)
    np.testing.assert_almost_equal(points.wind(), wind)
    np.testing.assert_almost_equal(np.median(points.wdir()), 270 - 180)
    np.testing.assert_almost_equal(np.median(points.wdir(angular=True)), np.pi)

    points.set_u(3)
    points.set_v(4)
    np.testing.assert_almost_equal(points.wind(), np.full(points.shape("u"), 5))

    points.set_u(4)
    points.set_v(3)
    np.testing.assert_almost_equal(points.wind(), np.full(points.shape("u"), 5))

    points.set_u(0)
    points.set_v(1)
    np.testing.assert_almost_equal(np.median(points.wdir()), 0 + 180)
    np.testing.assert_almost_equal(np.median(points.wdir(angular=True)), np.pi / 2)

    points.set_u(0)
    points.set_v(-1)
    np.testing.assert_almost_equal(np.median(points.wdir()), 180 - 180)
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

    np.testing.assert_almost_equal(np.median(points.wdir(empty=True)), 45 + 180)
    np.testing.assert_almost_equal(
        np.median(points.wdir(empty=True, angular=True)), np.pi / 4
    )

    points.set_u(-1)
    points.set_v(1)
    np.testing.assert_almost_equal(points.wind(), wind)
    np.testing.assert_almost_equal(np.median(points.wdir()), -45 + 360 - 180)
    np.testing.assert_almost_equal(np.median(points.wdir(angular=True)), np.pi * 3 / 4)

    points.set_u(2**0.5)
    points.set_v(0)
    np.testing.assert_almost_equal(points.wind(), wind)
    np.testing.assert_almost_equal(np.median(points.wdir()), 90 + 180)
    np.testing.assert_almost_equal(np.median(points.wdir(angular=True)), 0)

    points.set_u(-(2**0.5))
    points.set_v(0)
    np.testing.assert_almost_equal(points.wind(), wind)
    np.testing.assert_almost_equal(np.median(points.wdir()), 270 - 180)
    np.testing.assert_almost_equal(np.median(points.wdir(angular=True)), np.pi)

    points.set_u(3)
    points.set_v(4)
    np.testing.assert_almost_equal(points.wind(), np.full(points.shape("u"), 5))

    points.set_u(4)
    points.set_v(3)
    np.testing.assert_almost_equal(points.wind(), np.full(points.shape("u"), 5))

    points.set_u(0)
    points.set_v(1)
    np.testing.assert_almost_equal(np.median(points.wdir()), 0 + 180)
    np.testing.assert_almost_equal(np.median(points.wdir(angular=True)), np.pi / 2)

    points.set_u(0)
    points.set_v(-1)
    np.testing.assert_almost_equal(np.median(points.wdir()), 180 - 180)
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

    np.testing.assert_almost_equal(np.median(points.wdir(empty=True)), 45 + 180)
    np.testing.assert_almost_equal(
        np.median(points.wdir(empty=True, angular=True)), np.pi / 4
    )

    points.set_u(-1)
    points.set_v(1)
    np.testing.assert_almost_equal(points.wind(), wind)
    np.testing.assert_almost_equal(np.median(points.wdir()), -45 + 360 - 180)
    np.testing.assert_almost_equal(np.median(points.wdir(angular=True)), np.pi * 3 / 4)

    points.set_u(2**0.5)
    points.set_v(0)
    np.testing.assert_almost_equal(points.wind(), wind)
    np.testing.assert_almost_equal(np.median(points.wdir()), 90 + 180)
    np.testing.assert_almost_equal(np.median(points.wdir(angular=True)), 0)

    points.set_u(-(2**0.5))
    points.set_v(0)
    np.testing.assert_almost_equal(points.wind(), wind)
    np.testing.assert_almost_equal(np.median(points.wdir()), 270 - 180)
    np.testing.assert_almost_equal(np.median(points.wdir(angular=True)), np.pi)

    points.set_u(3)
    points.set_v(4)
    np.testing.assert_almost_equal(points.wind(), np.full(points.shape("u"), 5))

    points.set_u(4)
    points.set_v(3)
    np.testing.assert_almost_equal(points.wind(), np.full(points.shape("u"), 5))

    points.set_u(0)
    points.set_v(1)
    np.testing.assert_almost_equal(np.median(points.wdir()), 0 + 180)
    np.testing.assert_almost_equal(np.median(points.wdir(angular=True)), np.pi / 2)

    points.set_u(0)
    points.set_v(-1)
    np.testing.assert_almost_equal(np.median(points.wdir()), 180 - 180)
    np.testing.assert_almost_equal(np.median(points.wdir(angular=True)), -np.pi / 2)


def test_set_magnitude():
    @add_magnitude(name="wind", x="u", y="v", direction="wdir")
    @add_datavar("v", default_value=1)
    @add_datavar("u", default_value=1)
    class Magnitude(GriddedSkeleton):
        pass

    points = Magnitude(x=(0, 1, 2), y=(5, 6, 7, 8))
    points.deactivate_dask()
    u = np.full(points.size(), 2)
    ud = np.zeros(points.size())
    udm = ud - np.pi / 2
    points.set_wind(u, ud)
    np.testing.assert_almost_equal(u, points.wind())
    np.testing.assert_almost_equal(ud, points.wdir())
    np.testing.assert_almost_equal(udm, points.wdir(angular=True))
    points.set_wind(u, udm, angular=True)
    np.testing.assert_almost_equal(u, points.wind())
    np.testing.assert_almost_equal(ud, points.wdir())
    np.testing.assert_almost_equal(udm, points.wdir(angular=True))


def test_set_magnitude_dask():
    @add_magnitude(name="wind", x="u", y="v", direction="wdir")
    @add_datavar("v", default_value=1)
    @add_datavar("u", default_value=1)
    class Magnitude(GriddedSkeleton):
        pass

    points = Magnitude(x=(0, 1, 2), y=(5, 6, 7, 8))
    u = np.full(points.size(), 2)
    ud = np.zeros(points.size())
    udm = ud - np.pi / 2
    points.set_wind(u, ud)
    np.testing.assert_almost_equal(u, points.wind())
    np.testing.assert_almost_equal(ud, points.wdir())
    np.testing.assert_almost_equal(udm, points.wdir(angular=True))
    points.set_wind(u, udm, angular=True)
    np.testing.assert_almost_equal(u, points.wind())
    np.testing.assert_almost_equal(ud, points.wdir())
    np.testing.assert_almost_equal(udm, points.wdir(angular=True))


def test_set_magnitude_constant():
    @add_magnitude(name="wind", x="u", y="v", direction="wdir")
    @add_datavar("v", default_value=1)
    @add_datavar("u", default_value=1)
    class Magnitude(GriddedSkeleton):
        pass

    points = Magnitude(x=(0, 1, 2), y=(5, 6, 7, 8))
    u = np.full(points.size(), 2)
    ud = np.zeros(points.size())
    udm = ud - np.pi / 2
    points.set_wind(2, 0)
    np.testing.assert_almost_equal(u, points.wind())

    np.testing.assert_almost_equal(ud, points.wdir())
    np.testing.assert_almost_equal(udm, points.wdir(angular=True))
    points.set_wind(2, -np.pi / 2, angular=True)

    np.testing.assert_almost_equal(u, points.wind())
    np.testing.assert_almost_equal(ud, points.wdir())
    np.testing.assert_almost_equal(udm, points.wdir(angular=True))


def test_set_magnitude_constant_empty_direction():
    @add_magnitude(name="wind", x="u", y="v", direction="wdir")
    @add_datavar("v", default_value=1)
    @add_datavar("u", default_value=1)
    class Magnitude(GriddedSkeleton):
        pass

    points = Magnitude(x=(0, 1, 2), y=(5, 6, 7, 8))
    u = np.full(points.size(), 2)
    ud = np.zeros(points.size())
    udm = ud - np.pi / 2

    points.set_u(0)
    points.set_v(-2)
    np.testing.assert_almost_equal(u, points.wind())
    np.testing.assert_almost_equal(ud, points.wdir())
    np.testing.assert_almost_equal(udm, points.wdir(angular=True))

    points.set_wind(2)
    np.testing.assert_almost_equal(u, points.wind())
    np.testing.assert_almost_equal(ud, points.wdir())
    np.testing.assert_almost_equal(udm, points.wdir(angular=True))
    points.set_wind(2, angular=True)

    np.testing.assert_almost_equal(u, points.wind())
    np.testing.assert_almost_equal(ud, points.wdir())
    np.testing.assert_almost_equal(udm, points.wdir(angular=True))


def test_get_magnitude():
    @add_magnitude(name="wind", x="u", y="v", direction="wdir")
    @add_datavar("v", default_value=1)
    @add_datavar("u", default_value=1)
    class Magnitude(GriddedSkeleton):
        pass

    points = Magnitude(x=(0, 1, 2), y=(5, 6, 7, 8))
    points.set_u(0)
    points.set_v(3)

    np.testing.assert_almost_equal(
        points.wdir(dask=False), points.get("wdir", dask=False)
    )
    points.deactivate_dask()
    np.testing.assert_almost_equal(
        points.wdir(angular=True), points.get("wdir", angular=True)
    )
    np.testing.assert_almost_equal(points.wind(), points.get("wind"))

    np.testing.assert_almost_equal(
        points.wdir(empty=True), points.get("wdir", empty=True)
    )
    points.deactivate_dask()
    np.testing.assert_almost_equal(
        points.wdir(angular=True, empty=True),
        points.get("wdir", angular=True, empty=True),
    )
    np.testing.assert_almost_equal(
        points.wind(empty=True), points.get("wind", empty=True)
    )
    np.testing.assert_almost_equal(
        points.wdir(empty=True), np.full(points.shape("wdir"), 225)
    )


def test_scale_magnitude():
    @add_magnitude(name="wind", x="u", y="v", direction="wdir")
    @add_datavar("v", default_value=1)
    @add_datavar("u", default_value=1)
    class Magnitude(GriddedSkeleton):
        pass

    points = Magnitude(x=(0, 1, 2), y=(5, 6, 7, 8))
    points.deactivate_dask()
    points.set_u(3)
    points.set_v(3)
    umag = (3**2 + 3**2) ** 0.5
    ux = np.full(points.shape("wind"), 3)
    uy = np.full(points.shape("wind"), 3)
    u = np.full(points.shape("wind"), umag)
    ud = np.full(points.shape("wdir"), 225)

    np.testing.assert_almost_equal(points.wdir(), ud)
    np.testing.assert_almost_equal(points.wind(), u)

    points.set_wind(umag * 2)
    np.testing.assert_almost_equal(points.wind(), u * 2)
    np.testing.assert_almost_equal(points.wdir(), ud)
    np.testing.assert_almost_equal(points.wdir(), ud)
    np.testing.assert_almost_equal(points.u(), ux * 2)
    np.testing.assert_almost_equal(points.v(), uy * 2)


def test_turn_direction():
    @add_magnitude(name="wind", x="u", y="v", direction="wdir")
    @add_datavar("v", default_value=1)
    @add_datavar("u", default_value=1)
    class Magnitude(GriddedSkeleton):
        pass

    points = Magnitude(x=(0, 1, 2), y=(5, 6, 7, 8))
    points.deactivate_dask()
    points.set_u(3)
    points.set_v(3)
    ux = np.full(points.shape("wind"), 3)
    uy = np.full(points.shape("wind"), 3)
    umag = (3**2 + 3**2) ** 0.5
    u = np.full(points.shape("wind"), umag)
    ud = np.full(points.shape("wdir"), 225)

    np.testing.assert_almost_equal(points.wdir(), ud)
    np.testing.assert_almost_equal(points.wind(), u)

    points.set_wind(direction=(points.wdir() + 180))
    np.testing.assert_almost_equal(points.wind(), u)
    np.testing.assert_almost_equal(points.wdir(), ud - 180)
    np.testing.assert_almost_equal(points.u(), -ux)
    np.testing.assert_almost_equal(points.v(), -uy)
