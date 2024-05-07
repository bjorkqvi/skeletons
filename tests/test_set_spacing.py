from geo_skeletons.gridded_skeleton import GriddedSkeleton
from geo_skeletons.distance_funcs import lon_in_km, lat_in_km
import numpy as np


def test_nx_ny_cartesian():
    grid = GriddedSkeleton(x=(-2, 2), y=(-3, 3))
    grid.set_spacing(nx=5, ny=7)
    assert grid.nx() == 5
    assert grid.ny() == 7
    assert grid.size() == (7, 5)
    np.testing.assert_array_almost_equal(grid.x(), np.array([-2, -1, 0, 1, 2]))
    np.testing.assert_array_almost_equal(grid.y(), np.array([-3, -2, -1, 0, 1, 2, 3]))


def test_nx_ny_spherical():
    grid = GriddedSkeleton(lon=(-2, 2), lat=(0, 3))
    grid.set_spacing(nx=5, ny=4)
    assert grid.nx() == 5
    assert grid.ny() == 4
    assert grid.size() == (4, 5)
    np.testing.assert_array_almost_equal(grid.lon(), np.array([-2, -1, 0, 1, 2]))
    np.testing.assert_array_almost_equal(grid.lat(), np.array([0, 1, 2, 3]))


def test_dx_dy_cartesian():
    grid = GriddedSkeleton(x=(-1, 1), y=(-3, 3))
    grid.set_spacing(dx=0.5, dy=3)
    assert grid.nx() == 5
    assert grid.ny() == 3
    assert grid.size() == (3, 5)
    np.testing.assert_array_almost_equal(grid.x(), np.array([-1, -0.5, 0, 0.5, 1]))
    np.testing.assert_array_almost_equal(grid.y(), np.array([-3, 0, 3]))


def test_dm_cartesian():
    grid = GriddedSkeleton(x=(-1, 1), y=(-2, 2))
    grid.set_spacing(dm=0.5)
    assert grid.nx() == 5
    assert grid.ny() == 9
    assert grid.size() == (9, 5)
    np.testing.assert_array_almost_equal(grid.x(), np.array([-1, -0.5, 0, 0.5, 1]))
    np.testing.assert_array_almost_equal(
        grid.y(), np.array([-2, -1.5, -1, -0.5, 0, 0.5, 1, 1.5, 2])
    )


def test_dlon_dlat_spherical():
    grid = GriddedSkeleton(lon=(-1, 1), lat=(0, 3))
    grid.set_spacing(dlon=0.5, dlat=1.5)
    assert grid.nx() == 5
    assert grid.ny() == 3
    assert grid.size() == (3, 5)
    np.testing.assert_array_almost_equal(grid.lon(), np.array([-1, -0.5, 0, 0.5, 1]))
    np.testing.assert_array_almost_equal(grid.lat(), np.array([0, 1.5, 3]))


def test_dx_dy_spherical():
    grid = GriddedSkeleton(lon=(4, 5), lat=(60, 61))
    grid.set_spacing(dx=1110, dy=1110)
    dx = lon_in_km(60.5)
    dy = lat_in_km(60.5)
    np.testing.assert_array_almost_equal(grid.dlat(), 0.009, decimal=3)
    np.testing.assert_array_almost_equal(grid.dlon(), 0.017, decimal=3)
    np.testing.assert_array_almost_equal(grid.dy() / 1000, grid.dlat() * dy, decimal=1)
    np.testing.assert_array_almost_equal(grid.dx() / 1000, grid.dlon() * dx, decimal=1)
    assert grid.nx() == 1 / grid.dlon() + 1
    assert grid.ny() == 1 / grid.dlat() + 1


def test_dlon_dlat_cartesian():
    grid = GriddedSkeleton(x=(0, 150_000), y=(6_700_000, 6_800_000))
    grid.utm.set((33, "W"))
    grid.set_spacing(dlon=0.02, dlat=0.01)
    np.testing.assert_array_almost_equal(grid.dlat(), 0.01, decimal=2)
    np.testing.assert_array_almost_equal(grid.dlon(), 0.02, decimal=2)
    assert grid.nx() == 150_000 / grid.dx() + 1
    assert grid.ny() == 100_000 / grid.dy() + 1
    np.testing.assert_array_almost_equal(
        grid.dy(), 100_000 / (grid.ny() - 1), decimal=0
    )
    np.testing.assert_array_almost_equal(
        grid.dx(), 150_000 / (grid.nx() - 1), decimal=0
    )


def test_dlon_dlat_spherical_floating():
    grid = GriddedSkeleton(lon=(-1, 0.999), lat=(0, 2.999))
    grid.set_spacing(dlon=0.5, dlat=1.5, floating_edge=True)
    assert grid.nx() == 5
    assert grid.ny() == 3
    assert grid.size() == (3, 5)
    np.testing.assert_array_almost_equal(grid.lon(), np.array([-1, -0.5, 0, 0.5, 1]))
    np.testing.assert_array_almost_equal(grid.lat(), np.array([0, 1.5, 3]))


def test_dx_dy_cartesian_floating():
    grid = GriddedSkeleton(x=(-1, 0.999), y=(-3, 2.999))
    grid.set_spacing(dx=0.5, dy=3, floating_edge=True)
    assert grid.nx() == 5
    assert grid.ny() == 3
    assert grid.size() == (3, 5)
    np.testing.assert_array_almost_equal(grid.x(), np.array([-1, -0.5, 0, 0.5, 1]))
    np.testing.assert_array_almost_equal(grid.y(), np.array([-3, 0, 3]))


def test_dnmi_spherical():
    grid = GriddedSkeleton(lon=(2, 7), lat=(60, 61))
    grid.set_spacing(dnmi=1)
    dx = lon_in_km(60.5)
    dy = lat_in_km(60.5)

    np.testing.assert_array_almost_equal(grid.dlat(), 1 / 60, decimal=3)
    np.testing.assert_array_almost_equal(grid.dlon(), grid.dlat() * dy / dx, decimal=3)
    assert grid.nx() == (5 / grid.dlon()).astype(int) + 1
    assert grid.ny() == (1 / grid.dlat()).astype(int) + 1
    np.testing.assert_array_almost_equal(grid.dy() / 1000, dy / grid.ny(), decimal=1)
    np.testing.assert_array_almost_equal(
        grid.dx() / 1000, dx * 5 / grid.nx(), decimal=1
    )


def test_dnmi_cartesian():
    grid = GriddedSkeleton(x=(0, 150_000), y=(6_700_000, 6_800_000))
    grid.utm.set((33, "W"))
    grid.set_spacing(dnmi=0.5)
    np.testing.assert_array_almost_equal(grid.dy() / 1000, 1.85 / 2, decimal=2)
    np.testing.assert_array_almost_equal(grid.dx() / 1000, 1.85 / 2, decimal=2)
    assert grid.nx() == 150_000 / grid.dx() + 1
    assert grid.ny() == 100_000 / grid.dy() + 1
    dx = lon_in_km(np.median(grid.lat()))
    dy = lat_in_km(np.median(grid.lat()))
    np.testing.assert_array_almost_equal(grid.dlat(), 1 / 120, decimal=3)
    np.testing.assert_array_almost_equal(grid.dlon(), grid.dlat() * dy / dx, decimal=3)
