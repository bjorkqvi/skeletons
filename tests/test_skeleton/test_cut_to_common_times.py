from geo_skeletons import PointSkeleton, GriddedSkeleton
import numpy as np
import pytest
from geo_skeletons.errors import SkeletonError
def test_point_no_var():
    cls = PointSkeleton.add_time()
    data1 = cls(x=(10,29), y=(50,60), time=('2020-01-01 00:00', '2020-01-01 06:00'))
    data2 = cls(x=(11,23), y=(51,63), time=('2020-01-01 04:00', '2020-01-01 09:00'))

    data3, data4 = data1.cut_to_common_times(data2)

    times = ['2020-01-01 04:00:00', '2020-01-01 05:00:00', '2020-01-01 06:00:00']
    assert data3.time(datetime=False) == times
    assert data4.time(datetime=False) == times
    np.testing.assert_array_almost_equal(data1.x(), data3.x())
    np.testing.assert_array_almost_equal(data1.y(), data3.y())
    np.testing.assert_array_almost_equal(data2.x(), data4.x())
    np.testing.assert_array_almost_equal(data2.y(), data4.y())


def test_point():
    cls = PointSkeleton.add_time().add_datavar('hs')
    data1 = cls(x=(10,29), y=(50,60), time=('2020-01-01 00:00', '2020-01-01 06:00'))
    data2 = cls(x=(11,23), y=(51,63), time=('2020-01-01 04:00', '2020-01-01 09:00'))
    data1.set_hs(3)
    data2.set_hs(4)
    data3, data4 = data1.cut_to_common_times(data2)

    times = ['2020-01-01 04:00:00', '2020-01-01 05:00:00', '2020-01-01 06:00:00']
    assert data3.time(datetime=False) == times
    assert data4.time(datetime=False) == times
    np.testing.assert_array_almost_equal(data1.x(), data3.x())
    np.testing.assert_array_almost_equal(data1.y(), data3.y())
    np.testing.assert_array_almost_equal(data2.x(), data4.x())
    np.testing.assert_array_almost_equal(data2.y(), data4.y())
    np.testing.assert_array_almost_equal(data1.hs(), 3)
    np.testing.assert_array_almost_equal(data2.hs(), 4)
    np.testing.assert_array_almost_equal(data3.hs(), 3)
    np.testing.assert_array_almost_equal(data4.hs(), 4)


def test_gridded_no_var():
    cls = GriddedSkeleton.add_time()
    data1 = cls(x=(10,29), y=(50,60), time=('2020-01-01 00:00', '2020-01-01 06:00'))
    data2 = cls(x=(11,23), y=(51,63), time=('2020-01-01 04:00', '2020-01-01 09:00'))

    data3, data4 = data1.cut_to_common_times(data2)

    times = ['2020-01-01 04:00:00', '2020-01-01 05:00:00', '2020-01-01 06:00:00']
    assert data3.time(datetime=False) == times
    assert data4.time(datetime=False) == times
    np.testing.assert_array_almost_equal(data1.x(), data3.x())
    np.testing.assert_array_almost_equal(data1.y(), data3.y())
    np.testing.assert_array_almost_equal(data2.x(), data4.x())
    np.testing.assert_array_almost_equal(data2.y(), data4.y())


def test_gridded():
    cls = GriddedSkeleton.add_time().add_datavar('hs')
    data1 = cls(x=(10,29), y=(50,60), time=('2020-01-01 00:00', '2020-01-01 06:00'))
    data2 = cls(x=(11,23), y=(51,63), time=('2020-01-01 04:00', '2020-01-01 09:00'))
    data1.set_hs(3)
    data2.set_hs(4)
    data3, data4 = data1.cut_to_common_times(data2)

    times = ['2020-01-01 04:00:00', '2020-01-01 05:00:00', '2020-01-01 06:00:00']
    assert data3.time(datetime=False) == times
    assert data4.time(datetime=False) == times
    np.testing.assert_array_almost_equal(data1.x(), data3.x())
    np.testing.assert_array_almost_equal(data1.y(), data3.y())
    np.testing.assert_array_almost_equal(data2.x(), data4.x())
    np.testing.assert_array_almost_equal(data2.y(), data4.y())
    np.testing.assert_array_almost_equal(data1.hs(), 3)
    np.testing.assert_array_almost_equal(data2.hs(), 4)
    np.testing.assert_array_almost_equal(data3.hs(), 3)
    np.testing.assert_array_almost_equal(data4.hs(), 4)


def test_no_time():
    data1 = PointSkeleton(x=(10,29), y=(50,60))
    data2 = PointSkeleton.add_time()(x=(10,29), y=(50,60), time=('2020-01-01 00:00', '2020-01-01 06:00'))
    with pytest.raises(SkeletonError):
        a,b = data1.cut_to_common_times(data2)

    with pytest.raises(SkeletonError):
        a,b = data2.cut_to_common_times(data1)