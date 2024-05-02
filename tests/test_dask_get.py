import dask.array as da
import numpy as np
import xarray as xr
from geo_skeletons import PointSkeleton
from geo_skeletons.decorators import add_datavar
import pytest


"""Functionality to get dask arrays

Using dask = True/False always gets you a dask/numpy array no matter what

If dask keyword not give (=None), then:
In dask-mode: a dask array will be given
Not in dask-mode: you will get whatever is in the dataset

Logic is that we won't compute dask -> numpy unless explicitly instructed
since it is expensive and might crash python. In dask mode we won't have
to worry and will always get a dask array (numpy -> dask is a cheap operation)

NB! If dataset has a numpy array but a dask array is returned, then the 
data in the dataset is also converted to dask to not having to copy large data

xr is dask? dask-keyword    dask-mode active    Output is dask?
Y           None            Y                   Y
Y           True            Y                   Y
Y           False           Y                   N
---------------------------------------------------------------
Y           None            N                   Y
Y           True            N                   Y
Y           False           N                   N
---------------------------------------------------------------
N           None            Y                   Y               NB! Changes the ds-data to dask!
N           True            Y                   Y               NB! Changes the ds-data to dask!
N           False           Y                   N
---------------------------------------------------------------
N           None            N                   N
N           True            N                   Y               NB! Changes the ds-data to dask!
N           False           N                   N
"""


def data_is_dask(data) -> bool:
    """Checks if a data array is a dask array"""
    return hasattr(data, "chunks") and data.chunks is not None


@pytest.fixture
def wave_data():
    @add_datavar("hs")
    class WaveData(PointSkeleton):
        pass

    return WaveData(x=range(10), y=range(10))


def test_dask_array_in_ds(wave_data):
    data = da.from_array(np.zeros((10,)))
    points = wave_data
    points.dask.activate()
    points.set_hs(data)
    assert data_is_dask(points.ds().hs)

    assert data_is_dask(points.hs())
    assert data_is_dask(points.ds().hs)
    assert data_is_dask(points.hs(dask=True))
    assert data_is_dask(points.ds().hs)
    assert not data_is_dask(points.hs(dask=False))

    points.dask.deactivate()
    assert data_is_dask(points.ds().hs)
    assert data_is_dask(points.hs())
    assert data_is_dask(points.ds().hs)
    assert data_is_dask(points.hs(dask=True))
    assert data_is_dask(points.ds().hs)
    assert not data_is_dask(points.hs(dask=False))


def test_numpy_array_in_ds(wave_data):
    data = np.array(np.zeros((10,)))
    points = wave_data
    points.dask.deactivate()
    points.set_hs(data)
    assert not data_is_dask(points.ds().hs)

    points.dask.activate(rechunk=False)
    assert not data_is_dask(points.ds().hs)
    assert data_is_dask(points.hs())
    assert data_is_dask(points.ds().hs)  # NB! Changes the ds-data to dask!

    # Reset data in dataset to numpy to we can continue testing
    points.dask.deactivate()
    points.set_hs()
    points.dask.activate(rechunk=False)
    assert not data_is_dask(points.ds().hs)

    assert data_is_dask(points.hs(dask=True))
    assert data_is_dask(points.ds().hs)  # NB! Changes the ds-data to dask!

    # Reset data in dataset to numpy to we can continue testing
    points.dask.deactivate()
    points.set_hs()
    points.dask.activate(rechunk=False)
    assert not data_is_dask(points.ds().hs)

    assert not data_is_dask(points.hs(dask=False))

    points.dask.deactivate()
    assert not data_is_dask(points.ds().hs)

    assert not data_is_dask(points.hs())
    assert not data_is_dask(points.ds().hs)

    assert not data_is_dask(points.hs(dask=False))
    assert not data_is_dask(points.ds().hs)

    assert data_is_dask(points.hs(dask=True))
    assert data_is_dask(points.ds().hs)  # NB! Changes the ds-data to dask!
