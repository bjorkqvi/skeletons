from geo_skeletons import PointSkeleton, GriddedSkeleton
import numpy as np
import dask.array as da
from geo_skeletons import dask_computations as dc
import xarray as xr


def data_is_dask(data) -> bool:
    """Checks if a data array is a dask array"""
    return hasattr(data, "chunks") and data.chunks is not None


def test_reshape_me_np():
    data = np.zeros((1, 2, 3))
    assert dc.reshape_me(data, (0, 1, 2)).shape == data.shape
    assert dc.reshape_me(data, coord_order=(1, 0, 2)).shape == (2, 1, 3)
    assert not data_is_dask(dc.reshape_me(data, coord_order=(1, 0, 2)))


def test_reshape_me_da():
    data = da.from_array(np.zeros((1, 2, 3)))
    assert dc.reshape_me(data, (0, 1, 2)).shape == data.shape
    assert dc.reshape_me(data, coord_order=(1, 0, 2)).shape == (2, 1, 3)
    assert data_is_dask(dc.reshape_me(data, coord_order=(1, 0, 2)))


def test_expand_dims_np():
    data = np.zeros((2, 3, 4))
    assert dc.expand_dims(data, axis=(0, 2)).shape == (1, 2, 1, 3, 4)
    assert not data_is_dask(dc.expand_dims(data, axis=(0, 2)))


def test_expand_dims_da():
    data = da.from_array(np.zeros((2, 3, 4)))
    assert dc.expand_dims(data, axis=(0, 2)).shape == (1, 2, 1, 3, 4)
    assert data_is_dask(dc.expand_dims(data, axis=(0, 2)))


def test_cos_sin_np():
    data = np.array([0])
    np.testing.assert_almost_equal(dc.cos(data)[0], 1)
    np.testing.assert_almost_equal(dc.sin(data)[0], 0)
    assert not data_is_dask(dc.cos(data))
    assert not data_is_dask(dc.sin(data))


def test_cos_sin_da():
    data = da.from_array([0])
    np.testing.assert_almost_equal(dc.cos(data).compute()[0], 1)
    np.testing.assert_almost_equal(dc.sin(data).compute()[0], 0)
    assert data_is_dask(dc.cos(data))
    assert data_is_dask(dc.sin(data))


def test_mod_np():
    data = np.array([100])
    np.testing.assert_almost_equal(dc.mod(data, 90), 10)
    assert not data_is_dask(data)


def test_mod_da():
    data = da.from_array(np.array([100]))
    np.testing.assert_almost_equal(dc.mod(data, 90).compute(), 10)
    assert data_is_dask(data)


def test_arctan2_np():
    data = np.array([1])
    data2 = np.array([-1])
    np.testing.assert_almost_equal(dc.arctan2(data, data2), np.pi * 3 / 4)
    assert not data_is_dask(data)


def test_arctan2_da():
    data = da.from_array(np.array([1]))
    data2 = da.from_array(np.array([-1]))
    np.testing.assert_almost_equal(dc.arctan2(data, data2).compute(), np.pi * 3 / 4)
    assert data_is_dask(data)


def test_undask_me():
    data = np.array([1])
    assert not data_is_dask(dc.undask_me(data))
    data = da.from_array(data)
    assert not data_is_dask(dc.undask_me(data))
    assert dc.undask_me(None) is None


def test_atleast_1d_np():
    data = np.array(1)
    assert dc.atleast_1d(data).shape == (1,)
    assert not data_is_dask(dc.atleast_1d(data))

    data = np.array([1])
    assert dc.atleast_1d(data).shape == (1,)
    assert not data_is_dask(dc.atleast_1d(data))

    data = np.array([[1], [1]])
    assert dc.atleast_1d(data).shape == (2, 1)
    assert not data_is_dask(dc.atleast_1d(data))


def test_atleast_1d_da():
    data = da.from_array(np.array(1))
    assert dc.atleast_1d(data).compute().shape == (1,)
    assert data_is_dask(dc.atleast_1d(data))

    data = da.from_array(np.array([1]))
    assert dc.atleast_1d(data).compute().shape == (1,)
    assert data_is_dask(dc.atleast_1d(data))

    data = da.from_array(np.array([[1], [1]]))
    assert dc.atleast_1d(data).compute().shape == (2, 1)
    assert data_is_dask(dc.atleast_1d(data))


def test_atleast_1d_data_array_np():

    data = xr.DataArray(
        data=np.array([7, 8, 9]),
        dims=["inds"],
        coords=dict(
            inds=(["inds"], np.array([0, 1, 2])),
        ),
    )

    assert dc.atleast_1d(data).shape == (3,)
    assert isinstance(dc.atleast_1d(data), xr.DataArray)

    data = xr.DataArray(
        data=np.array([7]),
        dims=["inds"],
        coords=dict(
            inds=(["inds"], np.array([0])),
        ),
    )
    assert dc.atleast_1d(data.squeeze()).shape == (1,)
    assert isinstance(dc.atleast_1d(data), xr.DataArray)


def test_atleast_1d_data_array_da():

    data = xr.DataArray(
        data=da.from_array(np.array([7, 8, 9])),
        dims=["inds"],
        coords=dict(
            inds=(["inds"], np.array([0, 1, 2])),
        ),
    )

    assert dc.atleast_1d(data).shape == (3,)
    assert isinstance(dc.atleast_1d(data), xr.DataArray)
    assert data_is_dask(dc.atleast_1d(data).data)

    data = xr.DataArray(
        data=da.from_array(np.array([7])),
        dims=["inds"],
        coords=dict(
            inds=(["inds"], np.array([0])),
        ),
    )
    assert dc.atleast_1d(data.squeeze()).shape == (1,)
    assert isinstance(dc.atleast_1d(data), xr.DataArray)
    assert data_is_dask(dc.atleast_1d(data).data)
