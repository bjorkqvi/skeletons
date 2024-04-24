import numpy as np
from typing import Union
from copy import deepcopy
from functools import partial
from geo_parameters.metaparameter import MetaParameter
import geo_parameters as gp
import dask.array as da


def add_datavar(
    name: Union[str, MetaParameter],
    coords: str = "all",
    default_value: float = 0.0,
    dir_type: bool = None,
    append: bool = False,
):
    """stash_get = True means that the coordinate data can be accessed
    by method ._{name}() instead of .{name}()

    for directional parameters provide a MetaParameter or set:
    dir_type: 'from', 'to' or 'math'

    This allows for alternative definitions of the get-method elsewere."""

    def datavar_decorator(c):
        def get_var(
            self,
            empty: bool = False,
            strict: bool = False,
            dir_type: str = None,
            data_array: bool = False,
            squeeze: bool = True,
            dask: bool = None,
            **kwargs,
        ) -> np.ndarray:
            """Returns the data variable.

            Set empty=True to get an empty data variable (even if it doesn't exist).

            **kwargs can be used for slicing data.
            """
            var = self.get(
                name_str,
                empty=empty,
                strict=strict,
                dir_type=dir_type,
                data_array=data_array,
                squeeze=squeeze,
                dask=dask,
                **kwargs,
            )

            return var

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
        name_str, meta = gp.decode(name)
        c._coord_manager.add_var(name_str, meta, coords, default_value, dir_type)

        if append:
            exec(f"c.{name_str} = partial(get_var, c)")
            exec(f"c.set_{name_str} = partial(set_var, c)")
        else:
            exec(f"c.{name_str} = get_var")
            exec(f"c.set_{name_str} = set_var")

        return c

    # Always respect explicitly set directional convention
    # Otherwise parse from MetaParameter is possible
    # If dir_type is left to None, it means that this data variable is not a dirctional parameter
    if dir_type is None and gp.is_gp(name):
        standard_to = (
            "to_direction" in name.standard_name()
            or "to_direction" in name.standard_name(alias=True)
        )

        standard_from = (
            "from_direction" in name.standard_name()
            or "from_direction" in name.standard_name(alias=True)
        )
        if standard_to:
            dir_type = "to"
        elif standard_from:
            dir_type = "from"

    return datavar_decorator
