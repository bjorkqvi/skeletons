import numpy as np
from typing import Union
from copy import deepcopy
from functools import partial
from geo_parameters.metaparameter import MetaParameter


def add_datavar(
    name: Union[str, MetaParameter],
    coords: str = "all",
    default_value: float = 0.0,
    append: bool = False,
):
    """stash_get = True means that the coordinate data can be accessed
    by method ._{name}() instead of .{name}()

    This allows for alternative definitions of the get-method elsewere."""

    def datavar_decorator(c):
        def get_var(
            self,
            empty: bool = False,
            data_array: bool = False,
            squeeze: bool = False,
            dask: bool = None,
            **kwargs,
        ) -> np.ndarray:
            """Returns the data variable.

            Set empty=True to get an empty data variable (even if it doesn't exist).

            **kwargs can be used for slicing data.
            """
            if not self._structure_initialized():
                return None
            return self.get(
                name,
                empty=empty,
                data_array=data_array,
                squeeze=squeeze,
                dask=dask,
                **kwargs,
            )

        def set_var(
            self,
            data: Union[np.ndarray, int, float] = None,
            allow_reshape: bool = True,
            allow_transpose: bool = False,
            coords: list[str] = None,
            chunks: Union[tuple, str] = None,
            silent: bool = True,
        ) -> None:
            if isinstance(data, int) or isinstance(data, float):
                data = np.full(self.shape(name_str), data)
            self.set(
                name_str,
                data,
                allow_reshape=allow_reshape,
                allow_transpose=allow_transpose,
                coords=coords,
                chunks=chunks,
                silent=silent,
            )

        if c._coord_manager.initial_state:
            c._coord_manager = deepcopy(c._coord_manager)
            c._coord_manager.initial_state = False

        name_str = c._coord_manager.add_var(name, coords, default_value)

        if append:
            exec(f"c.{name_str} = partial(get_var, c)")
            exec(f"c.set_{name_str} = partial(set_var, c)")
        else:
            exec(f"c.{name_str} = get_var")
            exec(f"c.set_{name_str} = set_var")

        return c

    return datavar_decorator
