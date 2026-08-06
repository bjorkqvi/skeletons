import numpy as np
from typing import Union, Optional
from copy import deepcopy
from functools import partial
import dask.array as da
import xarray as xr
from geo_parameters.metaparameter import MetaParameter
import geo_parameters as gp
from geo_skeletons.variables import Magnitude, Direction
from geo_skeletons.errors import UnknownVariableError
import warnings




def add_magnitude(
    name: Union[str, MetaParameter],
    x: Optional[Union[str, MetaParameter]] = None,
    y: Optional[Union[str, MetaParameter]] = None,
    direction: Optional[Union[str, MetaParameter]] = None,
    dir_type: Optional[str] = None,
    disable_direction: bool = False
):
    """name Union[str, MetaParameter]: name of variable
    x Union[str, MetaParameter]: name of already set variable that will be used as x-component
    y Union[str, MetaParameter]: name of already set variable that will be used as y-component
    direction Optional[Union[str, MetaParameter]]: name of the direction of the magnitude being set
    dir_type Optional[str]: 'from', 'to' or 'math'

    x, y, and Direction are automatically decoded from the given parameter if it is a geo-parameter
    To disable the automatic decoding of the direction: disable_direction = True
    dir_type is decoded automatically if given direction is a metaparameter
    """

    def magnitude_decorator(c):
        def get_direction(
            self,
            empty: bool = False,
            data_array: bool = False,
            strict: bool = False,
            squeeze: bool = True,
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
            squeeze: bool = True,
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

        c.core = deepcopy(c.core)  # Makes a copy of the class coord_manager
        c.meta = c.core.meta

        name_str, meta = gp.decode(name)
        if direction is not None:
            dir_str, meta_dir = gp.decode(direction)
        else:
            dir_str, meta_dir = None, None

        # Find right names for the components if given as geo-parameters
        xstr, ystr = figure_out_components(c, name, x, y)


        coord_group = c.core.get(xstr).coord_group
        mag_obj = Magnitude(name=name_str, meta=meta, x=xstr, y=ystr, coord_group=coord_group)


        if direction is not None:


            dir_obj = Direction(
                name=dir_str,
                meta=meta_dir,
                x=xstr,
                y=ystr,
                coord_group=coord_group,
                dir_type=dir_type,
                magnitude=mag_obj,
            )
            mag_obj.direction = dir_obj

            c.core._add_direction(dir_obj)

            exec(f"c.{dir_str} = get_direction")
            exec(f"c.set_{dir_str} = set_direction")
        else:
            dir_str = None

        consistency_check_of_set_parameters(meta, x, y, direction, dir_type)

        exec(f"c.{name_str} = get_magnitude")
        exec(f"c.set_{name_str} = set_magnitude")

        c.core._add_magnitude(mag_obj)

        return c

    if dir_type not in ["to", "from", "math", None]:
        raise ValueError(
            f"'dir_type' needs to be 'to', 'from' or 'math' (or None), not {dir_type}"
        )


    # Try to decode direction from parameter family
    if gp.is_gp(name) and direction is None and not disable_direction:
        direction = name.my_family().get('direction')

    # Always respect explicitly set directional convention
    # Otherwise parse from MetaParameter is possible
    if dir_type is None and gp.is_gp(direction):
        dir_type = direction.dir_type()

    if dir_type is None and direction is not None:
        raise ValueError(
            f"Could not parse dir_type, please set it explicitly to 'from', 'to' or 'math'!"
        )

    return magnitude_decorator


def figure_out_components(cls, name: Union[str, MetaParameter], x: Union[str, MetaParameter], y: Union[str, MetaParameter]) -> tuple[str, str]:
    if gp.is_gp(name) and x is None:
        x = name.my_family().get('x')

    if gp.is_gp(name) and y is None:
        y = name.my_family().get('y')

    if gp.is_gp(x):
        xstr = cls.core.find(x)

        if len(xstr) == 0:
            raise UnknownVariableError(f"Cannot find a data variable matching parameter {x}!")
        if len(xstr) > 1:
            raise UnknownVariableError(f"Cannot find a unique data variable matching parameter {x}! (found {xstr})")
        xstr = xstr[0]
    else:
        xstr = x

    if gp.is_gp(y):
        ystr = cls.core.find(y)

        if len(ystr) == 0:
            raise UnknownVariableError(f"Cannot find a data variable matching parameter {y}!")
        if len(ystr) > 1:
            raise UnknownVariableError(f"Cannot find a unique data variable matching parameter {y}! (found {ystr})")
        ystr = ystr[0]
    else:
        ystr = y

    return xstr, ystr

def consistency_check_of_set_parameters(meta, x, y, direction, dir_type):
    """Warn if all the parameters are not consistent with what is expected based on the metaparameters"""
    if gp.is_gp(direction):
        if direction.dir_type() != dir_type:
            warnings.warn(f"The directional parameter {direction} has a directional type '{direction.dir_type()}', but the provided 'dir_type' is '{dir_type}'! It is highly recommended to keep the classes consistent.", Warning)
        if not direction.find_me_in([meta.my_family().get('direction'), meta.my_family().get('opposite_direction')]):
                warnings.warn(f"The parameter {direction} is not the known direction ({meta.my_family().get('direction')}) or opposite_direction ({meta.my_family().get('opposite_direction')}) of the parameter {meta}", Warning)
    
    if meta is None:
        return

    if gp.is_gp(x):
        if not x.find_me_in([meta.my_family().get('x')]):
            warnings.warn(f"The parameter {x} is not the known x-component ({meta.my_family().get('x')}) of the parameter {meta}!", Warning)

    if gp.is_gp(y):
        if not y.find_me_in([meta.my_family().get('y')]):
            warnings.warn(f"The parameter {y} is not the known y-component ({meta.my_family().get('y')}) of the parameter {meta}!", Warning)

    