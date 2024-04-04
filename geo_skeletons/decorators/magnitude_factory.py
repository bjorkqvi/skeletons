import numpy as np
from typing import Union
from copy import deepcopy
from functools import partial


def add_magnitude(
    name,
    x: str,
    y: str,
    default_value=0.0,
    append=False,
):
    """stash_get = True means that the coordinate data can be accessed
    by method ._{name}() instead of .{name}()

    This allows for alternative definitions of the get-method elsewere."""

    def magnitude_decorator(c):
        def get_magnitude(
            self,
            empty: bool = False,
            data_array: bool = False,
            squeeze: bool = False,
            **kwargs,
        ) -> np.ndarray:
            """Returns the magnitude.

            Set empty=True to get an empty data variable (even if it doesn't exist).

            **kwargs can be used for slicing data.
            """
            if not self._structure_initialized():
                return None
            xvar = self._coord_manager.magnitudes.get(name)["x"]
            yvar = self._coord_manager.magnitudes.get(name)["y"]
            x = self.get(
                xvar, empty=empty, data_array=data_array, squeeze=squeeze, **kwargs
            )
            y = self.get(
                yvar, empty=empty, data_array=data_array, squeeze=squeeze, **kwargs
            )

            return (x**2 + y**2) ** 0.5

        if c._coord_manager.initial_state:
            c._coord_manager = deepcopy(c._coord_manager)
            c._coord_manager.initial_state = False

        c._coord_manager.add_magnitude(name, x=x, y=y)

        if append:
            exec(f"c.{name} = partial(get_magnitude, c)")
        else:
            exec(f"c.{name} = get_magnitude")

        return c

    return magnitude_decorator
