from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .coordinate_manager import CoordinateManager
import xarray as xr


class MetaDataManager:
    def __init__(self, ds: xr.Dataset, coord_manager: CoordinateManager):
        self._ds = ds
        self._coord_manager = coord_manager
        self._metadata: dict = {}

    def set(
        self,
        metadata: dict,
        name: str = None,
    ) -> None:
        """Sets metadata to the class. Overwrites any old metadata."""
        if not isinstance(metadata, dict):
            raise TypeError(f"metadata needs to be a dict, not '{metadata}'!")

        # Store the metadata
        if name is not None:
            self._metadata[name] = metadata
        else:
            self._metadata["__general__"] = metadata

        # Store is dataset if possible
        if self._ds is not None and self._ds.get(name) is not None:
            self._ds_manager.set_attrs(metadata, name)

    def append(self, metadata: dict, name: str = None):
        """Appends metadata to the class"""
        old_metadata = self.get(name)
        old_metadata.update(metadata)
        self.set(old_metadata, name)

    def get(self, name: str = None) -> dict:
        """Return generic or data variable specific metadata"""
        if name is None:
            return self._metadata.get("__general__", {})

        metadata = self._metadata.get(name, {})

        if metadata:
            return metadata

        meta_parameter = self._coord_manager.meta_parameter(name)
        if meta_parameter is not None:
            return meta_parameter.meta_dict()
        return {}
