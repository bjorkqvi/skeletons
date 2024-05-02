import dask.array as da
import numpy as np
import xarray as xr
from geo_skeletons import PointSkeleton
from geo_skeletons.decorators import add_datavar
import pytest

"""Functionality to set dask arrays

If dask-mode is active OR chunks=... are explicitly provided, then data is stored in a dask-array.
If not, then even a provided dask-array will be converted.
To avoid possible computations of dask-to-numpy, have dask-mode activated.

Provided dask-array     dask-mode active    chunks-keyword given    Dask-array set in xr.Dataset
Y                       Y                   Y                       Y
Y                       Y                   N                       Y
Y                       N                   Y                       Y
Y                       N                   N                       N
------------------------------------------------------------------------------------------------
N                       Y                   Y                       Y
N                       Y                   N                       Y
N                       N                   Y                       Y
N                       N                   N                       N
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


def test_dask_array(wave_data):
    data = da.from_array(np.zeros((10,)))
    points = wave_data

    points.dask.activate()
    points.set_hs(data, chunks="auto")
    assert data_is_dask(points.ds().hs)

    points.set_hs(data)
    assert data_is_dask(points.ds().hs)

    points.dask.deactivate()
    points.set_hs(data, chunks="auto")
    assert data_is_dask(points.ds().hs)

    points.set_hs(data)
    assert not data_is_dask(points.ds().hs)


def test_numpy_array(wave_data):
    data = np.array(np.zeros((10,)))
    points = wave_data

    points.dask.activate()
    points.set_hs(data, chunks="auto")
    assert data_is_dask(points.ds().hs)

    points.set_hs(data)
    assert data_is_dask(points.ds().hs)

    points.dask.deactivate()
    points.set_hs(data, chunks="auto")
    assert data_is_dask(points.ds().hs)

    points.set_hs(data)
    assert not data_is_dask(points.ds().hs)
