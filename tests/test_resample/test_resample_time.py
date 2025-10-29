from geo_skeletons import PointSkeleton, GriddedSkeleton
from geo_skeletons.decorators import add_time, add_datavar, add_magnitude
import geo_parameters as gp
import numpy as np
import pandas as pd
import pytest


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
    data2 = data.resample.time(dt="1h", dropna=True)
    data3 = data.resample.time(dt="1h")

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


def test_2h():
    @add_datavar(gp.wave.Tp)
    @add_datavar(gp.wave.Dirp)
    @add_datavar(gp.wave.Hs)
    @add_time()
    class Wave(PointSkeleton):
        pass

    time = pd.date_range("2020-01-01 00:00", "2020-01-01 06:00", freq="10min")
    data = Wave(lon=0, lat=0, time=time)
    data.set_hs(5)
    data.set_dirp(90)
    data.set_tp(10)
    data2 = data.resample.time(dt="2h")
    assert len(data2.time()) == 4


def test_magnitude():
    @add_magnitude(gp.wind.Wind("mag"), x="u", y="v", direction=gp.wind.WindDir("dir"))
    @add_datavar(gp.wind.YWind("v"))
    @add_datavar(gp.wind.XWind("u"))
    @add_datavar(gp.wind.YGust("vg"))
    @add_datavar(gp.wind.XGust("ug"))
    @add_time()
    class Wind(PointSkeleton):
        pass

    lon, lat = np.array([1, 3, 4]), np.array([5, 6, 7])
    time = pd.date_range("2020-01-01 00:00", "2020-01-01 01:00", freq="10min")
    time1h = pd.date_range("2020-01-01 00:00", "2020-01-01 01:00", freq="1h")
    data = Wind(lon=lon, lat=lat, time=time)
    data.set_u(5)
    data.set_v(0)
    data.ind_insert("u", np.array([5, 5, 5, 5, -5, -5, -5]), inds=0)

    data.set_ug(5)
    data.set_vg(0)
    data.ind_insert("ug", np.array([5, 5, 5, 5, -5, -5, -5]), inds=0)
    data2 = data.resample.time(dt="1h")
    data3 = data.resample.time(dt="1h", dropna=False)
    assert [t.strftime("%Y%m%d%H%M") for t in data2.time()] == [
        t.strftime("%Y%m%d%H%M") for t in time1h
    ]
    assert [t.strftime("%Y%m%d%H%M") for t in data3.time()] == [
        t.strftime("%Y%m%d%H%M") for t in time1h
    ]

    assert np.all(np.isclose(data2.mag(), 5))
    # gusts averaged using components and will be wrong
    assert not np.all(np.isclose((data2.ug() ** 2 + data2.vg() ** 2) ** 0.5, 5))

    assert "through magnitude and direction" in data2.meta.get("u").get(
        "resample_method"
    )
    assert "through magnitude and direction" in data2.meta.get("v").get(
        "resample_method"
    )


def test_start_end():
    @add_datavar("hs")
    @add_time()
    class Wave(PointSkeleton):
        pass

    time = pd.date_range("2020-01-01 00:00", "2020-01-01 01:00", freq="10min")
    time30min = pd.date_range("2020-01-01 00:00", "2020-01-01 01:00", freq="30min")
    data = Wave(lon=0, lat=0, time=time)
    data.set_hs(np.arange(len(time)))
    data2 = data.resample.time(dt="30min")
    assert [t.strftime("%Y%m%d%H%M") for t in data2.time()] == [
        t.strftime("%Y%m%d%H%M") for t in time30min
    ]
    np.testing.assert_array_almost_equal(data2.hs(), np.array([1, 4, 6]))

    data3 = data.resample.time(dt="30min", mode="right")
    assert [t.strftime("%Y%m%d%H%M") for t in data3.time()] == [
        t.strftime("%Y%m%d%H%M") for t in time30min
    ]

    np.testing.assert_array_almost_equal(data3.hs(), np.array([0, 2, 5]))
    # aa = data.get('hs', data_array=True).resample(time=f"30min",closed='right', label='right', offset=pd.Timedelta(minutes=0)).reduce(np.mean)
    data4 = data.resample.time(dt="30min", mode="centered")
    assert [t.strftime("%Y%m%d%H%M") for t in data3.time()] == [
        t.strftime("%Y%m%d%H%M") for t in time30min
    ]
    np.testing.assert_array_almost_equal(data4.hs(), np.array([0.5, 3, 5.5]))
    assert "centered mean" in data4.meta.get("hs").get("resample_method")


def test_start_end_20min():
    @add_datavar("hs")
    @add_time()
    class Wave(PointSkeleton):
        pass

    time = pd.date_range("2020-01-01 00:00", "2020-01-01 01:00", freq="10min")
    time20min = pd.date_range("2020-01-01 00:00", "2020-01-01 01:00", freq="20min")
    data = Wave(lon=0, lat=0, time=time)
    data.set_hs(np.arange(len(time)))
    data2 = data.resample.time(dt="20min")
    assert [t.strftime("%Y%m%d%H%M") for t in data2.time()] == [
        t.strftime("%Y%m%d%H%M") for t in time20min
    ]
    np.testing.assert_array_almost_equal(data2.hs(), np.array([0.5, 2.5, 4.5, 6]))

    assert "left mean" in data2.meta.get("hs").get("resample_method")

    data3 = data.resample.time(dt="20min", mode="right")
    assert [t.strftime("%Y%m%d%H%M") for t in data3.time()] == [
        t.strftime("%Y%m%d%H%M") for t in time20min
    ]

    np.testing.assert_array_almost_equal(data3.hs(), np.array([0, 1.5, 3.5, 5.5]))
    assert "right mean" in data3.meta.get("hs").get("resample_method")
    with pytest.raises(ValueError):
        data4 = data.resample.time(dt="20min", mode="centered")


def test_start_end_hmax():
    @add_datavar(gp.wave.Hmax)
    @add_time()
    class Wave(PointSkeleton):
        pass

    time = pd.date_range("2020-01-01 00:00", "2020-01-01 01:00", freq="10min")
    time30min = pd.date_range("2020-01-01 00:00", "2020-01-01 01:00", freq="30min")
    data = Wave(lon=0, lat=0, time=time)
    data.set_hmax(np.arange(len(time)))
    data2 = data.resample.time(dt="30min")
    assert [t.strftime("%Y%m%d%H%M") for t in data2.time()] == [
        t.strftime("%Y%m%d%H%M") for t in time30min
    ]
    np.testing.assert_array_almost_equal(data2.hmax(), np.array([2, 5, 6]))

    data3 = data.resample.time(dt="30min", mode="right")
    assert [t.strftime("%Y%m%d%H%M") for t in data3.time()] == [
        t.strftime("%Y%m%d%H%M") for t in time30min
    ]

    np.testing.assert_array_almost_equal(data3.hmax(), np.array([0, 3, 6]))
    # aa = data.get('hs', data_array=True).resample(time=f"30min",closed='right', label='right', offset=pd.Timedelta(minutes=0)).reduce(np.mean)
    data4 = data.resample.time(dt="30min", mode="centered")
    assert [t.strftime("%Y%m%d%H%M") for t in data3.time()] == [
        t.strftime("%Y%m%d%H%M") for t in time30min
    ]
    np.testing.assert_array_almost_equal(data4.hmax(), np.array([1, 4, 6]))
    assert "centered mean" in data4.meta.get("hmax").get("resample_method")
    assert "np.max" in data4.meta.get("hmax").get("resample_method")


def test_start_end_hs():
    @add_datavar(gp.wave.Hs)
    @add_time()
    class Wave(PointSkeleton):
        pass

    time = pd.date_range("2020-01-01 00:00", "2020-01-01 01:00", freq="10min")
    time30min = pd.date_range("2020-01-01 00:00", "2020-01-01 01:00", freq="30min")
    data = Wave(lon=0, lat=0, time=time)
    data.set_hs(np.arange(len(time)))
    data2 = data.resample.time(dt="30min")
    assert [t.strftime("%Y%m%d%H%M") for t in data2.time()] == [
        t.strftime("%Y%m%d%H%M") for t in time30min
    ]

    x1 = (np.mean(np.array([0**2, 1**2, 2**2]))) ** 0.5
    x2 = (np.mean(np.array([3**2, 4**2, 5**2]))) ** 0.5
    x3 = (np.mean(np.array([6**2]))) ** 0.5
    np.testing.assert_array_almost_equal(data2.hs(), np.array([x1, x2, x3]))

    data3 = data.resample.time(dt="30min", mode="right")
    assert [t.strftime("%Y%m%d%H%M") for t in data3.time()] == [
        t.strftime("%Y%m%d%H%M") for t in time30min
    ]
    x1 = (np.mean(np.array([0**2]))) ** 0.5
    x2 = (np.mean(np.array([1**2, 2**2, 3**2]))) ** 0.5
    x3 = (np.mean(np.array([4**2, 5**2, 6**2]))) ** 0.5
    np.testing.assert_array_almost_equal(data3.hs(), np.array([x1, x2, x3]))
    # aa = data.get('hs', data_array=True).resample(time=f"30min",closed='right', label='right', offset=pd.Timedelta(minutes=0)).reduce(np.mean)
    data4 = data.resample.time(dt="30min", mode="centered")
    assert [t.strftime("%Y%m%d%H%M") for t in data3.time()] == [
        t.strftime("%Y%m%d%H%M") for t in time30min
    ]
    x1 = (np.mean(np.array([0**2, 1**2]))) ** 0.5
    x2 = (np.mean(np.array([2**2, 3**2, 4**2]))) ** 0.5
    x3 = (np.mean(np.array([5**2, 6**2]))) ** 0.5
    np.testing.assert_array_almost_equal(data4.hs(), np.array([x1, x2, x3]))
    assert "centered mean" in data4.meta.get("hs").get("resample_method")
    assert "np.sqrt(np.mean(x**2))" in data4.meta.get("hs").get("resample_method")
