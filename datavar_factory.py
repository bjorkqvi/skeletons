import numpy as np
from .coordinate_manager import CoordinateManager
from typing import Union


def add_datavar(
    name, coords="all", default_value=0.0, stash_get=False, aftermath=False
):
    """stash_get = True means that the coordinate data can be accessed
    by method ._{name}() instead of .{name}()

    This allows for alternative definitions of the get-method elsewere."""

    def datavar_decorator(c):
        def get_var(
            self, empty: bool = False, data_array: bool = False, **kwargs
        ) -> np.ndarray:
            """Returns the data variable.

            Set empty=True to get an empty data variable (even if it doesn't exist).

            **kwargs can be used for slicing data.
            """
            if not self._structure_initialized():
                return None
            if empty:
                return np.full(self.size(coords, **kwargs), default_value)

            data = self._ds_manager.get(name, **kwargs)
            if data_array:
                return data.copy()
            return data.values.copy()

        def set_var(self, data: Union[np.ndarray, int, float] = None) -> None:
            if isinstance(data, int) or isinstance(data, float):
                data = np.full(self._ds_manager.get(name).shape, data)
            self._update_datavar(name, data)

        if not hasattr(c, "_coord_manager"):
            c._coord_manager = CoordinateManager()

        c._coord_manager.add_var(name, coords)

        if stash_get:
            exec(f"c._{name} = get_var")
        else:
            if aftermath:
                exec(f"c.{name} = partial(get_var, c)")
                exec(f"c.set_{name} = partial(set_var, c)")
            else:
                exec(f"c.{name} = get_var")
                exec(f"c.set_{name} = set_var")

        return c

    return datavar_decorator
