from geo_skeletons import PointSkeleton, GriddedSkeleton
from geo_skeletons.decorators import add_datavar
import dask.array as da
import numpy as np


def data_is_dask(data) -> bool:
    return hasattr(data, "chunks")


def test_single_point_starting_with_numpy():
    """If a numpy array is given, then it is converted to a dask array, unlees dask-mode is deactivated.
    If dask-mode is deactivated, then the array is still converted if chunks are specified explicitly
    """

    @add_datavar(name="dummy", default_value=-9)
    class DummySkeleton(PointSkeleton):
        pass

    points = DummySkeleton(lon=1, lat=2)
    data = np.zeros((1,))

    points.set_dummy(data)
    assert data_is_dask(points.ds().dummy.data)
    assert data_is_dask(points.dummy())

    points.set_dummy(data, chunks="auto")
    assert data_is_dask(points.ds().dummy.data)
    assert data_is_dask(points.dummy())

    points.deactivate_dask()

    points.set_dummy(data)
    assert not data_is_dask(points.ds().dummy.data)
    assert not data_is_dask(points.dummy())

    points.set_dummy(data, chunks="auto")
    assert data_is_dask(points.ds().dummy.data)
    assert not data_is_dask(points.dummy())

    points.set_dummy(data)
    assert not data_is_dask(points.ds().dummy.data)
    assert not data_is_dask(points.dummy())

    points.activate_dask(rechunk=False)
    assert not data_is_dask(points.ds().dummy.data)
    assert not data_is_dask(points.dummy(dask=False))
    assert data_is_dask(points.dummy())
    assert data_is_dask(points.get("dummy"))

    points.activate_dask()
    assert data_is_dask(points.ds().dummy.data)
    assert data_is_dask(points.dummy())
    assert not data_is_dask(points.dummy(dask=False))

    points.set_dummy(data)
    assert data_is_dask(points.ds().dummy.data)
    assert data_is_dask(points.dummy())
    assert not data_is_dask(points.dummy(dask=False))


def test_single_point_starting_with_dask():
    """If a dask array is given, then it is always stored as a dask array"""

    @add_datavar(name="dummy", default_value=-9)
    class DummySkeleton(PointSkeleton):
        pass

    points = DummySkeleton(lon=1, lat=2)
    data = da.from_array(np.zeros((1,)))

    points.set_dummy(data)
    assert data_is_dask(points.ds().dummy.data)
    assert data_is_dask(points.dummy())

    points.set_dummy(data, chunks="auto")
    assert data_is_dask(points.ds().dummy.data)
    assert data_is_dask(points.dummy())

    points.deactivate_dask()

    points.set_dummy(data)
    assert data_is_dask(points.ds().dummy.data)
    assert not data_is_dask(points.dummy())

    points.set_dummy(data, chunks="auto")
    assert data_is_dask(points.ds().dummy.data)
    assert not data_is_dask(points.dummy())

    points.activate_dask()
    points.set_dummy(data)
    assert data_is_dask(points.ds().dummy.data)
    assert data_is_dask(points.dummy())
