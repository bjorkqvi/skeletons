from __future__ import annotations
from typing import TYPE_CHECKING, Union, Optional, Any

if TYPE_CHECKING:
    from .dataset_manager import DatasetManager

from copy import deepcopy


class MetaDataManager:
    def __init__(self, ds_manager: Union[DatasetManager, None]):
        self._ds_manager = ds_manager
        self._metadata: dict = {}
        # This will be used to make a deepcopy of the manager for different classes
        self._initial_state = True
        # This will be used to make a deepcopy of the manager for instances
        self._uninitialized = True

    def _ds_set_possible(self, name: Optional[str]):
        """Checks if it is possible to set metadata to the dataset"""
        if self._ds_manager is None:
            return False
        if self._ds_manager.ds() is None:
            return False
        if name is None:
            return True
        if self._ds_manager.get(name) is None:
            return False
        return True

    def set_by_dict(self, meta_dict: dict) -> None:
        """Sets the metadata by a dict where the key names are variable names"""
        for key, value in meta_dict.items():
            if key != "_global_":
                self.set(value, key)
            else:
                self.set(value)

    def set(
        self,
        metadata: dict[str, Any],
        name: Optional[str] = None,
    ) -> None:
        """Sets metadata to the class. Overwrites any old metadata.

        If 'name' is not given, the metadata is saved as general metadata not connected to any variable.
        """
        if not isinstance(metadata, dict):
            raise TypeError(f"metadata needs to be a dict, not '{metadata}'!")

        # Store the metadata
        if name is not None:
            self._metadata[name] = metadata
        else:
            self._metadata["_global_"] = metadata
        self.metadata_to_ds(name)

    def metadata_to_ds(self, name: Optional[str]) -> None:
        """Sets the stored metadata the the underlying dataset if possible"""
        if self._ds_set_possible(name):
            metadata = self.get(name)
            self._ds_manager.set_attrs(metadata, name)

    def append(
        self,
        metadata: dict[str, Any],
        name: Optional[str] = None,
    ):
        """Appends metadata to the class.

        If 'name' is not given, the metadata is saved as general metadata not connected to any variable.
        """
        old_metadata = self.get(name)

        old_metadata.update(metadata)
        self.set(old_metadata, name)

    def get(self, name: Optional[str] = None) -> dict[str, Any]:
        """Return metadata.

        If 'name' is not given, it return the metadata not connected to any variable.
        """
        if name is None:
            return deepcopy(self.meta_dict().get("_global_", {}))

        return deepcopy(self.meta_dict().get(name, {}))

    def meta_dict(self) -> dict:
        """Returns a dictonary of all the metadata"""
        return self._metadata
