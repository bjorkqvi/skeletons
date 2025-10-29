from geo_skeletons import PointSkeleton, GriddedSkeleton
from geo_skeletons.decorators import add_time, add_coord
import numpy as np
import pandas as pd


def test_plain_point():
    lon, lat = np.array([1, 3, 4]), np.array([5, 6, 7])
    points = PointSkeleton(lon=lon, lat=lat)

    assert set(points.coord_dict("gridpoint").keys()) == set({})
    assert set(points.coord_dict().keys()) == {"lon", "lat"}
    assert set(points.coord_dict("spatial").keys()) == {"lon", "lat"}
    assert set(points.coord_dict("grid").keys()) == {"lon", "lat"}

    assert set(points.coord_dict("nonspatial").keys()) == set({})
    np.testing.assert_array_almost_equal(lat, points.coord_dict().get("lat"))
    np.testing.assert_array_almost_equal(lon, points.coord_dict().get("lon"))


def test_plain_gridded():
    lon, lat = np.array([1, 3, 4]), np.array([5, 6, 7])
    points = GriddedSkeleton(lon=lon, lat=lat)

    assert set(points.coord_dict("gridpoint").keys()) == set({})
    assert set(points.coord_dict().keys()) == {"lon", "lat"}
    assert set(points.coord_dict("spatial").keys()) == {"lon", "lat"}
    assert set(points.coord_dict("grid").keys()) == {"lon", "lat"}

    assert set(points.coord_dict("nonspatial").keys()) == set({})
    np.testing.assert_array_almost_equal(lat, points.coord_dict().get("lat"))
    np.testing.assert_array_almost_equal(lon, points.coord_dict().get("lon"))


def test_time_point():
    @add_time(grid_coord=False)
    class Dummy(PointSkeleton):
        pass

    lon, lat = np.array([1, 3, 4]), np.array([5, 6, 7])
    time = pd.date_range("2020-01-01 00:00", "2020-01-01 01:00", freq="1h")
    points = Dummy(lon=lon, lat=lat, time=time)

    assert set(points.coord_dict("gridpoint").keys()) == set({"time"})
    assert set(points.coord_dict().keys()) == {"lon", "lat", "time"}
    assert set(points.coord_dict("spatial").keys()) == {"lon", "lat"}
    assert set(points.coord_dict("grid").keys()) == {"lon", "lat"}
    assert set(points.coord_dict("nonspatial").keys()) == set({"time"})

    np.testing.assert_array_almost_equal(lat, points.coord_dict().get("lat"))
    np.testing.assert_array_almost_equal(lon, points.coord_dict().get("lon"))
    assert [t.strftime("%Y%m%d%H%M") for t in time] == [
        t.strftime("%Y%m%d%H%M") for t in points.coord_dict().get("time")
    ]


def test_time_gridded():
    @add_time(grid_coord=True)
    class Dummy(GriddedSkeleton):
        pass

    lon, lat = np.array([1, 3, 4]), np.array([5, 6, 7])
    time = pd.date_range("2020-01-01 00:00", "2020-01-01 01:00", freq="1h")
    points = Dummy(lon=lon, lat=lat, time=time)

    assert set(points.coord_dict("gridpoint").keys()) == set({})
    assert set(points.coord_dict().keys()) == {"lon", "lat", "time"}
    assert set(points.coord_dict("spatial").keys()) == {"lon", "lat"}
    assert set(points.coord_dict("grid").keys()) == {"lon", "lat", "time"}
    assert set(points.coord_dict("nonspatial").keys()) == set({"time"})

    np.testing.assert_array_almost_equal(lat, points.coord_dict().get("lat"))
    np.testing.assert_array_almost_equal(lon, points.coord_dict().get("lon"))
    assert [t.strftime("%Y%m%d%H%M") for t in time] == [
        t.strftime("%Y%m%d%H%M") for t in points.coord_dict().get("time")
    ]


def test_addcoord_point():
    @add_coord("dummy", grid_coord=True)
    class Dummy(PointSkeleton):
        pass

    lon, lat = np.array([1, 3, 4]), np.array([5, 6, 7])
    dummy = np.array([7, 9])
    points = Dummy(lon=lon, lat=lat, dummy=dummy)

    assert set(points.coord_dict("gridpoint").keys()) == set({})
    assert set(points.coord_dict().keys()) == {"lon", "lat", "dummy"}
    assert set(points.coord_dict("spatial").keys()) == {"lon", "lat"}
    assert set(points.coord_dict("grid").keys()) == {"lon", "lat", "dummy"}
    assert set(points.coord_dict("nonspatial").keys()) == set({"dummy"})

    np.testing.assert_array_almost_equal(lat, points.coord_dict().get("lat"))
    np.testing.assert_array_almost_equal(lon, points.coord_dict().get("lon"))
    np.testing.assert_array_almost_equal(dummy, points.coord_dict().get("dummy"))


def test_addcoord_gridded():
    @add_coord("dummy", grid_coord=False)
    class Dummy(GriddedSkeleton):
        pass

    lon, lat = np.array([1, 3, 4]), np.array([5, 6, 7])
    dummy = np.array([7, 9])
    points = Dummy(lon=lon, lat=lat, dummy=dummy)

    assert set(points.coord_dict("gridpoint").keys()) == set({"dummy"})
    assert set(points.coord_dict().keys()) == {"lon", "lat", "dummy"}
    assert set(points.coord_dict("spatial").keys()) == {"lon", "lat"}
    assert set(points.coord_dict("grid").keys()) == {
        "lon",
        "lat",
    }
    assert set(points.coord_dict("nonspatial").keys()) == set({"dummy"})

    np.testing.assert_array_almost_equal(lat, points.coord_dict().get("lat"))
    np.testing.assert_array_almost_equal(lon, points.coord_dict().get("lon"))
    np.testing.assert_array_almost_equal(dummy, points.coord_dict().get("dummy"))
