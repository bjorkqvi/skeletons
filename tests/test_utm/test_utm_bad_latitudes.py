from geo_skeletons import GriddedSkeleton, PointSkeleton
import numpy as np


def test_point_high_lat():
    points = PointSkeleton(lon=(0, 1, 2, 3), lat=(80, 82, 84, 85))
    x, y = points.x(), points.y()
    assert np.isnan(x[-1])
    assert np.isnan(y[-1])

    x, y = points.xy()
    assert np.isnan(x[-1])
    assert np.isnan(y[-1])

    x, y = points.xgrid(), points.ygrid()
    assert np.isnan(x[-1])
    assert np.isnan(y[-1])


def test_point_low_lat():
    points = PointSkeleton(lon=(0, 1, 2, 3), lat=(-81, 80, 82, 84))
    x, y = points.x(), points.y()
    assert np.isnan(x[0])
    assert np.isnan(y[0])

    x, y = points.xy()
    assert np.isnan(x[0])
    assert np.isnan(y[0])

    x, y = points.xgrid(), points.ygrid()
    assert np.isnan(x[0])
    assert np.isnan(y[0])


def test_gridded_high_lat():
    points = GriddedSkeleton(lon=(0, 1, 2, 3), lat=(80, 82, 84, 85))
    x, y = points.x(), points.y()
    assert not np.any(np.isnan(x))
    assert np.isnan(y[-1])

    x, y = points.xy()
    assert np.all(np.isnan(x[-4:]))
    assert np.all(np.isnan(y[-4:]))

    x, y = points.xgrid(), points.ygrid()
    assert np.all(np.isnan(x[-1, :]))
    assert np.all(np.isnan(y[-1, :]))


def test_gridded_low_lat():
    points = GriddedSkeleton(lon=(0, 1, 2, 3), lat=(-81, 80, 82, 84))
    x, y = points.x(), points.y()
    assert not np.any(np.isnan(x))
    assert np.isnan(y[0])

    x, y = points.xy()
    assert np.all(np.isnan(x[0:4]))
    assert np.all(np.isnan(y[0:4]))

    x, y = points.xgrid(), points.ygrid()
    assert np.all(np.isnan(x[0, :]))
    assert np.all(np.isnan(y[0, :]))
