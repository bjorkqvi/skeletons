from geo_skeletons import GriddedSkeleton, PointSkeleton
import numpy as np


def test_spherical_gridded():
    grid = GriddedSkeleton(lon=(0, 1, 2, 3), lat=(50, 51, 52, 53))
    x, y = grid.x(), grid.y()
    xn, yn = grid.x(normalize=True), grid.y(normalize=True)
    np.testing.assert_almost_equal(np.min(xn), 0)
    np.testing.assert_almost_equal(np.min(yn), 0)
    np.testing.assert_array_almost_equal(np.diff(x), np.diff(xn))
    np.testing.assert_array_almost_equal(np.diff(y), np.diff(yn))

    x, y = grid.xy()
    xn, yn = grid.xy(normalize=True)
    np.testing.assert_almost_equal(np.min(xn), 0)
    np.testing.assert_almost_equal(np.min(yn), 0)
    np.testing.assert_array_almost_equal(np.diff(x), np.diff(xn))
    np.testing.assert_array_almost_equal(np.diff(y), np.diff(yn))

    x, y = grid.xgrid(), grid.ygrid()
    xn, yn = grid.xgrid(normalize=True), grid.ygrid(normalize=True)
    np.testing.assert_almost_equal(np.min(xn), 0)
    np.testing.assert_almost_equal(np.min(yn), 0)
    np.testing.assert_array_almost_equal(np.diff(x), np.diff(xn))
    np.testing.assert_array_almost_equal(np.diff(y), np.diff(yn))


def test_cartesian_gridded():
    grid = GriddedSkeleton(
        x=[291783, 361180, 430588, 500000],
        y=[5539708, 5650895, 5762100, 5873322],
        utm=(31, "U"),
    )
    x, y = grid.x(), grid.y()
    xn, yn = grid.x(normalize=True), grid.y(normalize=True)
    np.testing.assert_almost_equal(np.min(xn), 0)
    np.testing.assert_almost_equal(np.min(yn), 0)
    np.testing.assert_array_almost_equal(np.diff(x), np.diff(xn))
    np.testing.assert_array_almost_equal(np.diff(y), np.diff(yn))

    x, y = grid.xy()
    xn, yn = grid.xy(normalize=True)
    np.testing.assert_almost_equal(np.min(xn), 0)
    np.testing.assert_almost_equal(np.min(yn), 0)
    np.testing.assert_array_almost_equal(np.diff(x), np.diff(xn))
    np.testing.assert_array_almost_equal(np.diff(y), np.diff(yn))

    x, y = grid.xgrid(), grid.ygrid()
    xn, yn = grid.xgrid(normalize=True), grid.ygrid(normalize=True)
    np.testing.assert_almost_equal(np.min(xn), 0)
    np.testing.assert_almost_equal(np.min(yn), 0)
    np.testing.assert_array_almost_equal(np.diff(x), np.diff(xn))
    np.testing.assert_array_almost_equal(np.diff(y), np.diff(yn))


def test_spherical_point():
    grid = PointSkeleton(lon=(0, 1, 2, 3), lat=(50, 51, 52, 53))
    x, y = grid.x(), grid.y()
    xn, yn = grid.x(normalize=True), grid.y(normalize=True)
    np.testing.assert_almost_equal(np.min(xn), 0)
    np.testing.assert_almost_equal(np.min(yn), 0)
    np.testing.assert_array_almost_equal(np.diff(x), np.diff(xn))
    np.testing.assert_array_almost_equal(np.diff(y), np.diff(yn))

    x, y = grid.xy()
    xn, yn = grid.xy(normalize=True)
    np.testing.assert_almost_equal(np.min(xn), 0)
    np.testing.assert_almost_equal(np.min(yn), 0)
    np.testing.assert_array_almost_equal(np.diff(x), np.diff(xn))
    np.testing.assert_array_almost_equal(np.diff(y), np.diff(yn))

    x, y = grid.xgrid(), grid.ygrid()
    xn, yn = grid.xgrid(normalize=True), grid.ygrid(normalize=True)
    np.testing.assert_almost_equal(np.min(xn), 0)
    np.testing.assert_almost_equal(np.min(yn), 0)
    np.testing.assert_array_almost_equal(np.diff(x), np.diff(xn))
    np.testing.assert_array_almost_equal(np.diff(y), np.diff(yn))


def test_cartesian_point():
    grid = PointSkeleton(
        x=[291783, 361180, 430588, 500000],
        y=[5539708, 5650895, 5762100, 5873322],
        utm=(31, "U"),
    )
    x, y = grid.x(), grid.y()
    xn, yn = grid.x(normalize=True), grid.y(normalize=True)
    np.testing.assert_almost_equal(np.min(xn), 0)
    np.testing.assert_almost_equal(np.min(yn), 0)
    np.testing.assert_array_almost_equal(np.diff(x), np.diff(xn))
    np.testing.assert_array_almost_equal(np.diff(y), np.diff(yn))

    x, y = grid.xy()
    xn, yn = grid.xy(normalize=True)
    np.testing.assert_almost_equal(np.min(xn), 0)
    np.testing.assert_almost_equal(np.min(yn), 0)
    np.testing.assert_array_almost_equal(np.diff(x), np.diff(xn))
    np.testing.assert_array_almost_equal(np.diff(y), np.diff(yn))

    x, y = grid.xgrid(), grid.ygrid()
    xn, yn = grid.xgrid(normalize=True), grid.ygrid(normalize=True)
    np.testing.assert_almost_equal(np.min(xn), 0)
    np.testing.assert_almost_equal(np.min(yn), 0)
    np.testing.assert_array_almost_equal(np.diff(x), np.diff(xn))
    np.testing.assert_array_almost_equal(np.diff(y), np.diff(yn))
