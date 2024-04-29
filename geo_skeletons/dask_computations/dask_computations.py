import dask.array as da
import numpy as np
import xarray as xr


def reshape_me(data, coord_order):
    if data_is_dask(data):
        return da.transpose(data, coord_order)
    else:
        return np.transpose(data, coord_order)


def expand_dims(data, axis=tuple[int]):
    if data_is_dask(data):
        return da.expand_dims(data, axis=axis)
    else:
        return np.expand_dims(data, axis=axis)


def cos(data):
    if data_is_dask(data):
        return da.cos(data)
    else:
        return np.cos(data)


def sin(data):
    if data_is_dask(data):
        return da.sin(data)
    else:
        return np.sin(data)


def mod(data, mod):
    if data_is_dask(data):
        return da.mod(data, mod)
    else:
        return np.mod(data, mod)


def arctan2(y, x):
    if data_is_dask(y) and data_is_dask(x):
        return da.arctan2(y, x)
    else:
        return np.arctan2(y, x)


def atleast_1d(data):
    if data_is_dask(data):
        if not isinstance(data, xr.DataArray):
            return da.atleast_1d(data)
        else:
            data.data = da.atleast_1d(data)
            return data
    else:
        if not isinstance(data, xr.DataArray):
            return np.atleast_1d(data)
        else:
            data.data = np.atleast_1d(data)
            return data


def data_is_dask(data) -> bool:
    """Checks if a data array is a dask array"""
    return hasattr(data, "chunks") and data.chunks is not None


def undask_me(data):
    """Convert a dask array to a numpy array if needed"""
    if data is None:
        return None
    if data_is_dask(data):
        return data.compute()
    return data
