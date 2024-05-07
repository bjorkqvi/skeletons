import dask.array as da
import numpy as np
import xarray as xr

from typing import Union


def reshape_me(
    data: Union[np.ndarray, da.array], coord_order: tuple[int]
) -> Union[np.ndarray, da.array]:
    """Transpose a dask or numpy array"""
    if data_is_dask(data):
        return da.transpose(data, coord_order)
    else:
        return np.transpose(data, coord_order)


def expand_dims(
    data: Union[np.ndarray, da.array], axis=tuple[int]
) -> Union[np.ndarray, da.array]:
    """Expand the dimensions of a dask or numpy array"""
    if data_is_dask(data):
        return da.expand_dims(data, axis=axis)
    else:
        return np.expand_dims(data, axis=axis)


def cos(data: Union[np.ndarray, da.array]) -> Union[np.ndarray, da.array]:
    """cos on either dask or numpy array"""
    if data_is_dask(data):
        return da.cos(data)
    else:
        return np.cos(data)


def sin(data: Union[np.ndarray, da.array]) -> Union[np.ndarray, da.array]:
    """sin on either dask or numpy array"""
    if data_is_dask(data):
        return da.sin(data)
    else:
        return np.sin(data)


def mod(
    data: Union[np.ndarray, da.array], mod: Union[float, int]
) -> Union[np.ndarray, da.array]:
    """mod on either dask or numpy array"""
    if data_is_dask(data):
        return da.mod(data, mod)
    else:
        return np.mod(data, mod)


def arctan2(
    y: Union[np.ndarray, da.array], x: Union[np.ndarray, da.array]
) -> Union[np.ndarray, da.array]:
    """arctan2 on either dask or numpy array"""
    if data_is_dask(y) and data_is_dask(x):
        return da.arctan2(y, x)
    else:
        return np.arctan2(y, x)


def atleast_1d(
    data: Union[np.ndarray, da.array, xr.DataArray]
) -> Union[np.ndarray, da.array, xr.DataArray]:
    """atleadt_1d on either dask or numpy array"""
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


def data_is_dask(data: Union[np.ndarray, da.array, xr.DataArray]) -> bool:
    """Checks if a data array is a dask array"""
    return hasattr(data, "chunks") and data.chunks is not None


def undask_me(
    data: Union[np.ndarray, da.array, xr.DataArray]
) -> Union[np.ndarray, da.array, xr.DataArray]:
    """Convert a dask array to a numpy array if needed"""
    if data is None:
        return None
    if data_is_dask(data):
        return data.compute()
    return data
