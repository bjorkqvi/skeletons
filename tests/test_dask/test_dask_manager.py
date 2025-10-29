import dask.array as da
import numpy as np
import xarray as xr
from geo_skeletons.managers.dask_manager import DaskManager


def data_is_dask(data) -> bool:
    """Checks if a data array is a dask array"""
    return hasattr(data, "chunks") and data.chunks is not None


def test_numpy():
    data = np.zeros((100, 100))
    dask_manager = DaskManager(chunks="auto", skeleton=None)

    assert not data_is_dask(data)
    assert data_is_dask(data) == dask_manager.data_is_dask(data)

    np.testing.assert_almost_equal(data, dask_manager.undask_me(data))

    assert data_is_dask(dask_manager.dask_me(data))
    assert not data_is_dask(dask_manager.undask_me(data))

    dask_manager = DaskManager(chunks=(10, 10), skeleton=None)
    assert dask_manager.dask_me(data).chunksize == (10, 10)


def test_dask_array():
    data = da.from_array(np.zeros((100, 100)))
    assert data.chunksize == (100, 100)

    dask_manager = DaskManager(chunks=None, skeleton=None)

    assert data_is_dask(data)
    assert data_is_dask(data) == dask_manager.data_is_dask(data)

    np.testing.assert_almost_equal(data.compute(), dask_manager.undask_me(data))
    assert data_is_dask(dask_manager.dask_me(data))
    assert not data_is_dask(dask_manager.undask_me(data))

    dask_manager2 = DaskManager(chunks=(10, 10), skeleton=None)
    assert data_is_dask(dask_manager2.dask_me(data))
    assert not data_is_dask(dask_manager2.undask_me(data))

    assert dask_manager2.dask_me(data).chunksize == (100, 100)
    assert dask_manager2.dask_me(data, chunks=(10, 10)).chunksize == (10, 10)


def test_data_array():
    data = np.zeros((100, 100))
    daa = xr.DataArray(
        data=data,
        dims=["x", "y"],
        coords=dict(
            x=(["x"], range(100)),
            y=(["y"], range(100)),
        ),
    )

    dask_manager = DaskManager(skeleton=None)

    assert not data_is_dask(daa)
    assert data_is_dask(daa) == dask_manager.data_is_dask(daa)

    np.testing.assert_almost_equal(daa.data, dask_manager.undask_me(daa).data)
    assert data_is_dask(dask_manager.dask_me(daa))
    assert not data_is_dask(dask_manager.undask_me(daa))

    dask_manager = DaskManager(chunks=(10, 10), skeleton=None)
    assert not data_is_dask(dask_manager.undask_me(daa))
    assert data_is_dask(dask_manager.dask_me(daa))
    assert dask_manager.dask_me(daa, chunks=(10, 10)).data.chunksize == (10, 10)


def test_dask_data_array():
    data = da.from_array(np.zeros((100, 100)))
    daa = xr.DataArray(
        data=data,
        dims=["x", "y"],
        coords=dict(
            x=(["x"], range(100)),
            y=(["y"], range(100)),
        ),
    )

    dask_manager = DaskManager(chunks=None, skeleton=None)

    assert data_is_dask(daa)
    assert data_is_dask(daa) == dask_manager.data_is_dask(daa)

    np.testing.assert_almost_equal(daa.data.compute(), dask_manager.undask_me(daa).data)
    assert data_is_dask(dask_manager.dask_me(daa))
    assert not data_is_dask(dask_manager.undask_me(daa))

    dask_manager = DaskManager(chunks=(10, 10), skeleton=None)
    assert not data_is_dask(dask_manager.undask_me(daa))
    assert data_is_dask(dask_manager.dask_me(daa))
    assert dask_manager.dask_me(daa, chunks=(10, 10)).data.chunksize == (10, 10)
