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

    def _metadata_to_ds(self, name: Optional[str]) -> None:
        """Sets the stored metadata the the underlying dataset if possible"""
        if self._ds_set_possible(name):
            metadata = self.get(name)
            self._ds_manager.set_attrs(metadata, name)

    def set_by_dict(self, meta_dict: dict) -> None:
        """Sets metadata for multiple variables using a dictionary.

        This method sets metadata for multiple variables in the class. The keys in the 
        dictionary represent the variable names, and the corresponding values are the 
        metadata dictionaries to be set for those variables.

        Args:
            meta_dict (dict): A dictionary where:
                - Keys are variable names (str).
                - Values are metadata dictionaries (dict).
                - The special key `"_global_"` can be used to set general metadata not 
                associated with any specific variable.

        Returns:
            None: This method modifies the metadata in place.

        Notes:
            - Metadata for each variable is set using the `set` method.
            - If the key is `"_global_"`, the corresponding value is set as general metadata.
        """
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
        """Sets metadata for a specific variable or globally.

        This method sets metadata for a specific variable, overwriting any existing metadata 
        for that variable. If no variable name is provided, the metadata is stored as general 
        metadata (not associated with any variable).

        Args:
            metadata (dict[str, Any]): The metadata to set. Must be a dictionary.
            name (Optional[str], optional): The name of the variable to associate with the 
                metadata. If `None`, the metadata is stored as general metadata. Defaults to `None`.

        Returns:
            None: This method modifies the metadata in place.

        Raises:
            TypeError: If the `metadata` argument is not a dictionary.

        Notes:
            - If `name` is provided and the variable already has a `grid_mapping` set, 
            the `grid_mapping` is preserved and not overwritten.
        """
        if not isinstance(metadata, dict):
            raise TypeError(f"metadata needs to be a dict, not '{metadata}'!")

        # Store the metadata
        if name is not None:
            # Once grid mapping is set, we don't want to replace it
            grid_mapping = self._metadata.get(name,{}).get('grid_mapping', metadata.get('grid_mapping'))
            self._metadata[name] = metadata
            if grid_mapping is not None:
                self._metadata[name]['grid_mapping'] = grid_mapping

        else:
            self._metadata["_global_"] = metadata
        self._metadata_to_ds(name)



    def append(
        self,
        metadata: dict[str, Any],
        name: Optional[str] = None,
    ):
        """Appends (updates) metadata for a specific variable or globally.

        This method appends or updates metadata for a specific variable. If no variable name 
        is provided, the metadata is appended to the general metadata.

        Args:
            metadata (dict[str, Any]): The metadata to append. Must be a dictionary.
            name (Optional[str], optional): The name of the variable to associate with the 
                metadata. If `None`, the metadata is appended to the general metadata. Defaults to `None`.

        Returns:
            None: This method modifies the metadata in place.

        Notes:
            - Possible existing metadata is retrieved and updated with the new metadata.
        """
        old_metadata = self.get(name)

        old_metadata.update(metadata)
        self.set(old_metadata, name)

    def get(self, name: Optional[str] = None) -> dict[str, Any]:
        """Returns metadata for a specific variable or globally.

        This method retrieves the metadata associated with a specific variable. If no 
        variable name is provided, it returns the general metadata.

        Args:
            name (Optional[str], optional): The name of the variable to retrieve metadata for. 
                If `None`, the general metadata is returned. Defaults to `None`.

        Returns:
            dict[str, Any]: A dictionary containing the metadata for the specified variable 
            or the general metadata if `name` is `None`.

        Notes:
            - The returned dictionary is a deep copy of the stored metadata to prevent 
            unintended modifications.
            - If no metadata exists for the specified variable, an empty dictionary is returned.
        """
        if name is None:
            return deepcopy(self.meta_dict().get("_global_", {}))

        return deepcopy(self.meta_dict().get(name, {}))

    def meta_dict(self) -> dict:
        """Returns a dictionary containing all metadata.

        This method retrieves all metadata stored in the class, including metadata associated 
        with specific variables and general metadata.

        Returns:
            dict: A dictionary where:
                - Keys are variable names (str) or `"_global_"` for general metadata.
                - Values are metadata dictionaries (dict).

        Notes:
            - The metadata dictionary is a direct reference to the internal storage. Use the 
            `get` method to retrieve a deep copy for safe access.
        """
        return self._metadata
