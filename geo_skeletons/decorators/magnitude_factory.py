import numpy as np
from typing import Union, Optional
from copy import deepcopy
from functools import partial
import dask.array as da
import xarray as xr
from geo_parameters.metaparameter import MetaParameter
import geo_parameters as gp
from geo_skeletons.variables import Magnitude, Direction


def add_magnitude(
    name: Union[str, MetaParameter],
    x: str,
    y: str,
    direction: Optional[Union[str, MetaParameter]] = None,
    dir_type: Optional[str] = None,
    append: bool = False,
):
    """stash_get = True means that the coordinate data can be accessed
    by method ._{name}() instead of .{name}()

    dir_type: 'from', 'to' or 'math'

    This allows for alternative definitions of the get-method elsewere."""

    def magnitude_decorator(c):
        def get_direction(
            self,
            empty: bool = False,
            data_array: bool = False,
            strict: bool = False,
            squeeze: bool = False,
            dask: Optional[bool] = None,
            dir_type: Optional[str] = None,
            **kwargs,
        ) -> Union[np.ndarray, da.array, xr.DataArray]:
            """Returns the magnitude.

            Set empty=True to get an empty data variable (even if it doesn't exist).

            **kwargs can be used for slicing data.
            """
            var = self.get(
                dir_str,
                empty=empty,
                strict=strict,
                dir_type=dir_type,
                data_array=data_array,
                squeeze=squeeze,
                dask=dask,
                **kwargs,
            )

            return var

        def get_magnitude(
            self,
            empty: bool = False,
            data_array: bool = False,
            strict: bool = False,
            squeeze: bool = False,
            dask: Optional[bool] = None,
            **kwargs,
        ) -> Union[np.ndarray, da.array, xr.DataArray]:
            """Returns the magnitude.

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

        def set_magnitude(
            self,
            magnitude: Optional[Union[np.ndarray, int, float]] = None,
            allow_reshape: bool = True,
            allow_transpose: bool = False,
            coords: Optional[list[str]] = None,
            chunks: Optional[Union[tuple, str]] = None,
            silent: bool = True,
        ):
            self.set(
                name_str,
                data=magnitude,
                allow_reshape=allow_reshape,
                allow_transpose=allow_transpose,
                coords=coords,
                chunks=chunks,
                silent=silent,
            )

        def set_direction(
            self,
            direction: Optional[Union[np.ndarray, int, float]] = None,
            dir_type: Optional[str] = None,
            allow_reshape: bool = True,
            allow_transpose: bool = False,
            coords: Optional[list[str]] = None,
            chunks: Optional[Union[tuple, str]] = None,
            silent: bool = True,
        ):
            self.set(
                dir_str,
                data=direction,
                dir_type=dir_type,
                allow_reshape=allow_reshape,
                allow_transpose=allow_transpose,
                coords=coords,
                chunks=chunks,
                silent=silent,
            )

        if not c.core._is_altered():
            c.core = deepcopy(c.core)  # Makes a copy of the class coord_manager
            c.meta = deepcopy(c.meta)
            c.meta._coord_manager = c.core
        name_str, meta = gp.decode(name)
        if direction is not None:
            dir_str, meta_dir = gp.decode(direction)
        else:
            dir_str, meta_dir = None, None

        coord_group = c.core.get(x).coord_group
        mag_obj = Magnitude(name=name_str, meta=meta, x=x, y=y, coord_group=coord_group)

        if direction is not None:
            dir_obj = Direction(
                name=dir_str,
                meta=meta_dir,
                x=x,
                y=y,
                coord_group=coord_group,
                dir_type=dir_type,
                magnitude=mag_obj,
            )
            mag_obj.direction = dir_obj
            c.core.add_direction(dir_obj)
            if append:
                exec(f"c.{dir_str} = partial(get_direction, c)")
                exec(f"c.set_{dir_str} = partial(set_direction, c)")
            else:
                exec(f"c.{dir_str} = get_direction")
                exec(f"c.set_{dir_str} = set_direction")
        else:
            dir_str = None

        if append:
            exec(f"c.{name_str} = partial(get_magnitude, c)")
            exec(f"c.set_{name_str} = partial(set_magnitude, c)")
        else:
            exec(f"c.{name_str} = get_magnitude")
            exec(f"c.set_{name_str} = set_magnitude")

        c.core.add_magnitude(mag_obj)

        return c

    if dir_type not in ["to", "from", "math", None]:
        raise ValueError(
            f"'dir_type' needs to be 'to', 'from' or 'math' (or None), not {dir_type}"
        )

    # Always respect explicitly set directional convention
    # Otherwise parse from MetaParameter is possible
    if dir_type is None and gp.is_gp(direction):
        standard_to = (
            "to_direction" in direction.standard_name()
            or "to_direction" in direction.standard_name(alias=True)
        )

        standard_from = (
            "from_direction" in direction.standard_name()
            or "from_direction" in direction.standard_name(alias=True)
        )
        if standard_to:
            dir_type = "to"
        elif standard_from:
            dir_type = "from"

    if dir_type is None and direction is not None:
        raise ValueError(
            f"Could not parse dir_type, please set it explicitly to 'from', 'to' or 'math'!"
        )

    offset = {"from": 180, "to": 0}

    return magnitude_decorator
