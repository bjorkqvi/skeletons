import numpy as np


class ReshapeManager:
    def __init__(
        self,
        dask_manager,
        silent: bool = True,
    ) -> None:
        self.dask_manager = dask_manager
        self.silent = silent

    def explicit_reshape(self, data, data_coords, expected_coords):
        if expected_coords is None:
            return data

        if data is None:
            return None

        # Check that we don't do trivial reshape
        if data_coords == expected_coords:
            return data

        # Create a list of shapes based on the given coordinates
        coord_order = [
            expected_coords.index(c) for c in data_coords if c in expected_coords
        ]

        original_shape = data.shape
        data = self.dask_manager.reshape_me(data, tuple(coord_order))
        if not self.silent:
            print(
                f"Reshaping data {original_shape} -> {data.shape}: {expected_coords} -> {data_coords}"
            )

        return data

    def transpose_2d(self, data, expected_squeezed_shape: tuple[int]):
        """Transposes given data if it is a two dimensional transpose of the wanted size after removing all trivial dimension.

        If sizes match, it just squeezes the data.

        Returns None if reshaping is not possible"""
        # Check if the base data (ignoring any trivial dimensions is the right (or possibly transposed) dimensions
        if data is None:
            return None

        data = data.squeeze()
        actual_squeezed_shape = data.shape

        if expected_squeezed_shape == actual_squeezed_shape:
            return data

        # If data is not 2D and doesn't match, we don't want to try to reshape
        if len(actual_squeezed_shape) != 2 or len(expected_squeezed_shape) != 2:
            return None

        # Is the squeezed shape a transpose of the expected squeezed shape?
        if tuple(np.flip(actual_squeezed_shape)) == expected_squeezed_shape:
            return data.squeeze().T

    def unsqueeze(self, data, expected_shape: tuple[int]):
        """Unsqueezes the data by inserting trivial dimensions at the right places
        Returns None if not possible"""
        if data is None:
            return None

        data = data.squeeze()
        actual_shape = data.shape
        if actual_shape == expected_shape:
            return data

        trivial_places = tuple(np.where(np.array(expected_shape) == 1)[0])

        data = self.dask_manager.expand_dims(data, axis=trivial_places)

        if expected_shape != data.shape:
            return None

        return data
