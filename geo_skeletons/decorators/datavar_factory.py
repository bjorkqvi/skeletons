import numpy as np
from typing import Union, Optional
from copy import deepcopy
from functools import partial
from geo_parameters.metaparameter import MetaParameter
import geo_parameters as gp
import dask.array as da
import xarray as xr
from geo_skeletons.variables import DataVar


def add_datavar(
    name: Union[str, MetaParameter],
    coord_group: str = "all",
    default_value: float = 0.0,
    dir_type: Optional[bool] = None,
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
            dir_type: Optional[str] = None,
            data_array: bool = False,
            squeeze: bool = True,
            dask: Optional[bool] = None,
            **kwargs,
        ) -> Union[np.ndarray, da.array, xr.DataArray]:
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
            data: Optional[Union[np.ndarray, int, float]] = None,
            allow_reshape: bool = True,
            allow_transpose: bool = False,
            coords: Optional[list[str]] = None,
            chunks: Optional[Union[tuple, str]] = None,
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

        name_str, meta = gp.decode(name)

        data_var = DataVar(
            name=name_str,
            meta=meta,
            coord_group=coord_group,
            default_value=default_value,
            dir_type=dir_type,
        )

        if not c.core._is_altered():
            c.core = deepcopy(c.core)  # Makes a copy of the class coord_manager
            c.meta = deepcopy(c.meta)
            c.meta._coord_manager = c.core
        c.core.add_var(data_var)

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
    if dir_type not in ["to", "from", "math", None]:
        raise ValueError(
            f"'dir_type' needs to be 'to', 'from' or 'math' (or None), not {dir_type}"
        )

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
