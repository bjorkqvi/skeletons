from geo_skeletons import PointSkeleton, GriddedSkeleton
from geo_skeletons.decorators import add_time
import pandas as pd
import pytest
import numpy as np


@pytest.fixture
def timedata():
    @add_time()
    class TimeData(PointSkeleton):
        pass

    return TimeData


@pytest.fixture
def timegrid():
    @add_time()
    class TimeGrid(GriddedSkeleton):
        pass

    return TimeGrid


def test_time_from_tuple(timedata):
    points = timedata(lon=0, lat=0, time=("2020-01-01 00:00", "2020-01-02 23:00"))
    assert len(points.time()) == 48
    np.testing.assert_almost_equal(points.dt(), 1)
    assert points.time(datetime=False)[0] == "2020-01-01 00:00:00"
    assert points.time(datetime=False)[-1] == "2020-01-02 23:00:00"


def test_time_from_tuple_change_dt(timedata):
    points = timedata(lon=0, lat=0, time=("2020-01-01 00:00", "2020-01-02 23:00", "6h"))
    assert len(points.time()) == 48 / 6
    np.testing.assert_almost_equal(points.dt(), 6)
    assert points.time(datetime=False)[0] == "2020-01-01 00:00:00"
    assert points.time(datetime=False)[-1] == "2020-01-02 18:00:00"


def test_time_from_tuple_change_dt_half_hour(timedata):
    points = timedata(
        lon=0, lat=0, time=("2020-01-01 00:00", "2020-01-02 23:00", "30min")
    )
    assert len(points.time()) == 48 * 2 - 1
    np.testing.assert_almost_equal(points.dt(), 0.5)
    assert points.time(datetime=False)[0] == "2020-01-01 00:00:00"
    assert points.time(datetime=False)[-1] == "2020-01-02 23:00:00"


def test_gridded_time_from_tuple(timegrid):
    points = timegrid(lon=0, lat=0, time=("2020-01-01 00:00", "2020-01-02 23:00"))
    assert len(points.time()) == 48
    np.testing.assert_almost_equal(points.dt(), 1)
    assert points.time(datetime=False)[0] == "2020-01-01 00:00:00"
    assert points.time(datetime=False)[-1] == "2020-01-02 23:00:00"


def test_gridded_time_from_tuple_change_dt(timegrid):
    points = timegrid(lon=0, lat=0, time=("2020-01-01 00:00", "2020-01-02 23:00", "6h"))
    assert len(points.time()) == 48 / 6
    np.testing.assert_almost_equal(points.dt(), 6)
    assert points.time(datetime=False)[0] == "2020-01-01 00:00:00"
    assert points.time(datetime=False)[-1] == "2020-01-02 18:00:00"


def test_gridded_time_from_tuple_change_dt_half_hour(timegrid):
    points = timegrid(
        lon=0, lat=0, time=("2020-01-01 00:00", "2020-01-02 23:00", "30min")
    )
    assert len(points.time()) == 48 * 2 - 1
    np.testing.assert_almost_equal(points.dt(), 0.5)
    assert points.time(datetime=False)[0] == "2020-01-01 00:00:00"
    assert points.time(datetime=False)[-1] == "2020-01-02 23:00:00"
