from geo_skeletons import PointSkeleton, GriddedSkeleton
from geo_skeletons.decorators import add_time, add_datavar
import geo_parameters as gp
import numpy as np
import pandas as pd


def test_point():
    @add_datavar("hs")
    @add_time()
    class Wave(PointSkeleton):
        pass

    lon, lat = np.array([1, 3, 4]), np.array([5, 6, 7])
    time = pd.date_range("2020-01-01 00:00", "2020-01-01 06:00", freq="10min")
    time1h = pd.date_range("2020-01-01 00:00", "2020-01-01 06:00", freq="1h")
    data = Wave(lon=lon, lat=lat, time=time)
    data.set_hs(5)
    data2 = data.resample.time(dt="1h")
    data3 = data.resample.time(dt="1h", dropna=False)
    assert [t.strftime("%Y%m%d%H%M") for t in data2.time()] == [
        t.strftime("%Y%m%d%H%M") for t in time1h
    ]
    assert [t.strftime("%Y%m%d%H%M") for t in data3.time()] == [
        t.strftime("%Y%m%d%H%M") for t in time1h
    ]


def test_point_missing_time():
    @add_datavar("hs")
    @add_time()
    class Wave(PointSkeleton):
        pass

    lon, lat = np.array([1, 3, 4]), np.array([5, 6, 7])
    time = pd.date_range("2020-01-01 00:00", "2020-01-01 06:00", freq="10min")
    # Missing entire hour 01
    time = time.to_list()
    time[6:12] = []
    time = pd.to_datetime(time)
    #
    time1h = pd.date_range("2020-01-01 00:00", "2020-01-01 06:00", freq="1h")
    time1h_miss1 = time1h.to_list()

    del time1h_miss1[1]
    time1h_miss1 = pd.to_datetime(time1h_miss1)

    data = Wave(lon=lon, lat=lat, time=time)
    data.set_hs(5)
    data2 = data.resample.time(dt="1h")
    data3 = data.resample.time(dt="1h", dropna=False)
    assert [t.strftime("%Y%m%d%H%M") for t in data2.time()] == [
        t.strftime("%Y%m%d%H%M") for t in time1h_miss1
    ]
    assert [t.strftime("%Y%m%d%H%M") for t in data3.time()] == [
        t.strftime("%Y%m%d%H%M") for t in time1h
    ]


def test_point_gp():
    @add_datavar(gp.wave.Tp)
    @add_datavar(gp.wave.Dirp)
    @add_datavar(gp.wave.Hs)
    @add_time()
    class Wave(PointSkeleton):
        pass

    lon, lat = np.array([1, 3, 4]), np.array([5, 6, 7])
    time = pd.date_range("2020-01-01 00:00", "2020-01-01 06:00", freq="10min")
    time1h = pd.date_range("2020-01-01 00:00", "2020-01-01 06:00", freq="1h")
    data = Wave(lon=lon, lat=lat, time=time)
    data.set_hs(5)
    data.set_dirp(90)
    data.ind_insert(
        "dirp", np.array([90, 180, 270, 90, 180, 270]), inds=0, time=slice(0, 6)
    )
    data.ind_insert(
        "dirp", np.array([90, 0, 270, 90, 0, 270]), inds=0, time=slice(6, 12)
    )
    data.ind_insert(
        "dirp", np.array([90, 0, 45, 90, 0, 45]), inds=0, time=slice(12, 18)
    )
    data.ind_insert(
        "dirp", np.array([315, 0, 45, 315, 0, 45]), inds=0, time=slice(18, 24)
    )
    data.ind_insert(
        "dirp", np.array([0, 90, 180, 0, 90, 180]), inds=0, time=slice(24, 30)
    )
    data.ind_insert(
        "dirp", np.array([315, 0, 45, 0, 90, 180]), inds=0, time=slice(30, 36)
    )
    data.set_tp(10)
    data.ind_insert("tp", 5, inds=0, time=slice(0, 3))

    data2 = data.resample.time(dt="1h")
    data3 = data.resample.time(dt="1h", dropna=False)
    assert [t.strftime("%Y%m%d%H%M") for t in data2.time()] == [
        t.strftime("%Y%m%d%H%M") for t in time1h
    ]
    assert [t.strftime("%Y%m%d%H%M") for t in data3.time()] == [
        t.strftime("%Y%m%d%H%M") for t in time1h
    ]
    np.testing.assert_array_almost_equal(
        data2.dirp(inds=0), np.array([180.0, 0.0, 45.0, 0.0, 90.0, 22.5, 90.0])
    )
    np.testing.assert_array_almost_equal(
        data3.dirp(inds=0), np.array([180.0, 0.0, 45.0, 0.0, 90.0, 22.5, 90.0])
    )
    np.testing.assert_almost_equal(
        data2.tp(inds=0)[0], 1 / (np.mean(1 / (np.array([5, 5, 5, 10, 10, 10]))))
    )
    np.testing.assert_almost_equal(
        data3.tp(inds=0)[0], 1 / (np.mean(1 / (np.array([5, 5, 5, 10, 10, 10]))))
    )


def test_point_gp_dirtype_to():
    @add_datavar(gp.wave.DirpTo)
    @add_datavar(gp.wave.Hs)
    @add_time()
    class Wave(PointSkeleton):
        pass

    lon, lat = np.array([1, 3, 4]), np.array([5, 6, 7])
    time = pd.date_range("2020-01-01 00:00", "2020-01-01 06:00", freq="10min")
    time1h = pd.date_range("2020-01-01 00:00", "2020-01-01 06:00", freq="1h")
    data = Wave(lon=lon, lat=lat, time=time)
    data.set_hs(5)
    data.set_dirp(90)
    data.ind_insert(
        "dirp", np.array([90, 180, 270, 90, 180, 270]), inds=0, time=slice(0, 6)
    )
    data.ind_insert(
        "dirp", np.array([90, 0, 270, 90, 0, 270]), inds=0, time=slice(6, 12)
    )
    data.ind_insert(
        "dirp", np.array([90, 0, 45, 90, 0, 45]), inds=0, time=slice(12, 18)
    )
    data.ind_insert(
        "dirp", np.array([315, 0, 45, 315, 0, 45]), inds=0, time=slice(18, 24)
    )
    data.ind_insert(
        "dirp", np.array([0, 90, 180, 0, 90, 180]), inds=0, time=slice(24, 30)
    )
    data.ind_insert(
        "dirp", np.array([315, 0, 45, 0, 90, 180]), inds=0, time=slice(30, 36)
    )

    data2 = data.resample.time(dt="1h")
    data3 = data.resample.time(dt="1h", dropna=False)
    assert [t.strftime("%Y%m%d%H%M") for t in data2.time()] == [
        t.strftime("%Y%m%d%H%M") for t in time1h
    ]
    assert [t.strftime("%Y%m%d%H%M") for t in data3.time()] == [
        t.strftime("%Y%m%d%H%M") for t in time1h
    ]
    np.testing.assert_array_almost_equal(
        data2.dirp(inds=0), np.array([180.0, 0.0, 45.0, 0.0, 90.0, 22.5, 90.0])
    )
    np.testing.assert_array_almost_equal(
        data3.dirp(inds=0), np.array([180.0, 0.0, 45.0, 0.0, 90.0, 22.5, 90.0])
    )
