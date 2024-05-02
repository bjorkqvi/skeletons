import dask.array as da
import xarray as xr
from typing import Union
import numpy as np

from geo_skeletons.dask_computations import data_is_dask


class DaskManager:
    def __init__(self, skeleton, chunks="auto"):
        self.chunks = chunks
        self._skeleton = skeleton

    def activate(
        self, chunks="auto", primary_dim: str = None, rechunk: bool = True
    ) -> None:
        self.chunks = chunks
        if rechunk:
            self.rechunk(chunks, primary_dim)

    def deactivate(self, dechunk: bool = False) -> None:
        """Deactivates the use of dask, meaning:

        1) Data will not be converted to dask-arrays when set, unless chunks provided
        2) Data will be converted from dask-arrays to numpy arrays when get
        3) All data will be converted to numpy arrays if unchunk=True"""
        self.chunks = None

        if dechunk:
            self.dechunk()

    def rechunk(
        self,
        chunks: Union[tuple, dict, str] = "auto",
        primary_dim: Union[str, list[str]] = None,
    ) -> None:
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
        dask_manager = DaskManager()
        for var in self._skeleton.core.data_vars():
            data = self._skeleton.get(var)
            if data is not None:
                self._skeleton.set(var, dask_manager.undask_me(data))
        for var in self._skeleton.core.masks():
            data = self._skeleton.get(var)
            if data is not None:
                self._skeleton.set(var, dask_manager.undask_me(data))

    def is_active(self) -> bool:
        return self.chunks is not None

    @staticmethod
    def data_is_dask(data) -> bool:
        """Checks if a data array is a dask array"""
        return data_is_dask(data)

    def dask_me(self, data, chunks=None, force: bool = False):
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

    def undask_me(self, data):
        """Convert a dask array to a numpy array if needed"""
        if data is None:
            return None
        if self.data_is_dask(data):
            data = data.compute()
        return data

    def constant_array(
        self, data, shape: tuple[int], chunks: tuple[int]
    ) -> Union[da.array, np.array]:
        """Creates an dask or numpy array of a certain shape is given data is shapeless."""
        chunks = chunks or self.chunks
        use_dask = chunks is not None

        if data.shape != (1,) or data.shape == shape:
            return data

        if use_dask or self.data_is_dask(data):
            return da.full(shape, data[0], chunks=chunks or "auto")
        else:
            return np.full(shape, data)
