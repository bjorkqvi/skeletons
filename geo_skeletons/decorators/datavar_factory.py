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
):
    """name: name of variable
    coord_group: 'all', 'spatial', 'grid' or 'gridpoint'
    default_value: float
    dir_type (for directional parameters): 'from', 'to' or 'math' (Autimatically parsed if name is a MetaParameter)

    """

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
            dir_type: Optional[str] = None,
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
                dir_type=dir_type,
                allow_reshape=allow_reshape,
                allow_transpose=allow_transpose,
                coords=coords,
                chunks=chunks,
                silent=silent,
            )

        name_str, meta = gp.decode(name)

        if (
            meta is not None
            and meta.i_am() in ["x", "y"]
            and np.isclose(default_value, 0)
        ):
            def_val = 0.1
        else:
            def_val = default_value

        data_var = DataVar(
            name=name_str,
            meta=meta,
            coord_group=coord_group,
            default_value=def_val,
            dir_type=dir_type,
        )

        c.core = deepcopy(c.core)  # Makes a copy of the class coord_manager
        c.meta = c.core.meta

        # Temporarily cahnge core to dynamic if being set by decorator

        c.core.add_var(data_var)

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
        dir_type = name.dir_type()

    return datavar_decorator
