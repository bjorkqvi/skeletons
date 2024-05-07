from __future__ import annotations
from typing import TYPE_CHECKING, Optional, Union

if TYPE_CHECKING:
    from ..skeleton import Skeleton
import dask.array as da
import xarray as xr
import numpy as np

from geo_skeletons.dask_computations import data_is_dask


class DaskManager:
    def __init__(self, skeleton: Skeleton, chunks: Union[tuple[int], str] = "auto"):
        self.chunks = chunks
        self._skeleton = skeleton

    def activate(
        self,
        chunks: Union[tuple[int], str] = "auto",
        primary_dim: Optional[str] = None,
        rechunk: bool = True,
    ) -> None:
        """Activates dask-mode and rechunks the data unless 'rechunk' is set to False"""
        self.chunks = chunks
        if rechunk:
            self.rechunk(chunks, primary_dim)

    def deactivate(self, dechunk: bool = False) -> None:
        """Deactivates dask-mode. Data converted to numpy arrays if 'dechunk' set to True"""
        self.chunks = None

        if dechunk:
            self.dechunk()

    def rechunk(
        self,
        chunks: Union[tuple[int], dict[str, int], str] = "auto",
        primary_dim: Union[str, list[str]] = None,
    ) -> None:
        """Rechunks all the data"""
        if self.chunks is None:
            raise ValueError(
                "Dask mode is not activated! use .activate_dask() before rechunking"
            )
        if primary_dim:
            if isinstance(primary_dim, str):
                primary_dim = [primary_dim]
            chunks = {}
            for dim in primary_dim:
                chunks[dim] = len(self._skeleton.get(dim))

        if isinstance(chunks, dict):
            chunks = self._skeleton._chunk_tuple_from_dict(chunks)
        for var in self._skeleton.core.data_vars():
            data = self._skeleton.get(var, strict=True)
            if data is not None:
                self._skeleton.set(var, self.dask_me(data, chunks))
        for var in self._skeleton.core.masks():
            data = self._skeleton.get(var, strict=True)
            if data is not None:
                self._skeleton.set(var, self.dask_me(data, chunks))

    def dechunk(self) -> None:
        """Computes all dask arrays and coverts them to numpy arrays.

        If data is big this might taka a long time or kill Python."""
        for var in self._skeleton.core.data_vars():
            data = self._skeleton.get(var)
            if data is not None:
                self._skeleton.set(var, self.undask_me(data))
        for var in self._skeleton.core.masks():
            data = self._skeleton.get(var)
            if data is not None:
                self._skeleton.set(var, self.undask_me(data))

    def is_active(self) -> bool:
        """Checks if dask-mode is activated"""
        return self.chunks is not None

    @staticmethod
    def data_is_dask(data: Union[np.ndarray, da.array, xr.DataArray]) -> bool:
        """Checks if a data array is a dask array (or contains a dask array)"""
        return data_is_dask(data)

    def dask_me(
        self,
        data: Union[np.ndarray, da.array, xr.DataArray],
        chunks: Optional[Union[tuple[int], str]] = None,
        force: bool = False,
    ) -> Union[np.ndarray, da.array, xr.DataArray]:
        """Convert a numpy array to a dask array if needed and wanted

        If dask-mode is activated: returns dask array with set chunking
            - Override set chunking with chunks = ...

        If dask-mode is deactivate: return numpy array if numpy array is given
            - Dask is applied if chunks = ... is provided

        force = True: Always return a dask array no matter what
            - Use given chunks = ... or set chunks or 'auto'"""

        if data is None:
            return None

        if self.data_is_dask(data):
            # Rechunk already dasked data if explicitly requested
            if chunks is not None:
                if not isinstance(data, xr.DataArray):
                    data = data.rechunk(chunks)
                else:
                    data.data = data.data.rechunk(chunks)

            return data

        if force:
            chunks = chunks or self.chunks or "auto"

        if self.is_active() or chunks:
            if not isinstance(data, xr.DataArray):
                return da.from_array(data, chunks=chunks or self.chunks)
            else:
                data.data = da.from_array(data.data, chunks=chunks or self.chunks)
            return data

        return data

    def undask_me(
        self, data: Union[np.ndarray, da.array, xr.DataArray]
    ) -> Union[np.ndarray, da.array, xr.DataArray]:
        """Convert a dask array to a numpy array if needed"""
        if data is None:
            return None
        if self.data_is_dask(data):
            data = data.compute()
        return data

    def constant_array(
        self, data, shape: tuple[int], chunks: Union[tuple[int], str]
    ) -> Union[da.array, np.ndarray]:
        """Creates an dask or numpy array of a certain shape is given data is shapeless."""
        chunks = chunks or self.chunks
        use_dask = chunks is not None

        if data.shape != (1,) or data.shape == shape:
            return data

        if use_dask or self.data_is_dask(data):
            return da.full(shape, data[0], chunks=chunks or "auto")
        else:
            return np.full(shape, data)
