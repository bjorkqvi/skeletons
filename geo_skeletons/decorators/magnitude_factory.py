import numpy as np
from typing import Union
from copy import deepcopy
from functools import partial
import dask.array as da
from geo_parameters.metaparameter import MetaParameter
from ..managers.dask_manager import DaskManager
import geo_parameters as gp


def add_magnitude(
    name: Union[str, MetaParameter],
    x: str,
    y: str,
    direction: Union[str, MetaParameter] = None,
    dir_type: str = None,
    append=False,
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
            squeeze: bool = False,
            dask: bool = None,
            dir_type: str = None,
            **kwargs,
        ) -> np.ndarray:
            """Returns the magnitude.

            Set empty=True to get an empty data variable (even if it doesn't exist).

            **kwargs can be used for slicing data.
            """
            if not self._structure_initialized():
                return None
            xvar = self._coord_manager.magnitudes.get(name_str)["x"]
            yvar = self._coord_manager.magnitudes.get(name_str)["y"]
            x = self.get(
                xvar,
                empty=empty,
                data_array=data_array,
                squeeze=squeeze,
                dask=dask,
                **kwargs,
            )
            y = self.get(
                yvar,
                empty=empty,
                data_array=data_array,
                squeeze=squeeze,
                dask=dask,
                **kwargs,
            )

            if not empty and x is None or y is None:
                return None

            if x is None:
                x = self.get(
                    xvar,
                    empty=True,
                    data_array=data_array,
                    squeeze=squeeze,
                    dask=dask,
                    **kwargs,
                )

            if y is None:
                y = self.get(
                    yvar,
                    empty=True,
                    data_array=data_array,
                    squeeze=squeeze,
                    dask=dask,
                    **kwargs,
                )

            dir_type = dir_type or self._coord_manager.directions[dir_str]["dir_type"]

            if dask:
                dirs = da.arctan2(y, x)
            else:
                dirs = np.arctan2(y, x)

            if dir_type != "math":
                dirs = 90 - dirs * 180 / np.pi + offset[dir_type]
                if dask:
                    dirs = da.mod(dirs, 360)
                else:
                    dirs = np.mod(dirs, 360)

            return dirs

        def get_magnitude(
            self,
            empty: bool = False,
            data_array: bool = False,
            squeeze: bool = False,
            dask: bool = None,
            **kwargs,
        ) -> np.ndarray:
            """Returns the magnitude.

            Set empty=True to get an empty data variable (even if it doesn't exist).

            **kwargs can be used for slicing data.
            """
            if not self._structure_initialized():
                return None

            xvar = self._coord_manager.magnitudes.get(name_str)["x"]
            yvar = self._coord_manager.magnitudes.get(name_str)["y"]
            x = self.get(
                xvar,
                empty=empty,
                data_array=data_array,
                squeeze=squeeze,
                dask=dask,
                **kwargs,
            )
            y = self.get(
                yvar,
                empty=empty,
                data_array=data_array,
                squeeze=squeeze,
                dask=dask,
                **kwargs,
            )

            if not empty and x is None or y is None:
                return None

            if x is None:
                x = self.get(
                    xvar,
                    empty=True,
                    data_array=data_array,
                    squeeze=squeeze,
                    dask=dask,
                    **kwargs,
                )

            if y is None:
                y = self.get(
                    yvar,
                    empty=True,
                    data_array=data_array,
                    squeeze=squeeze,
                    dask=dask,
                    **kwargs,
                )

            return (x**2 + y**2) ** 0.5

        def set_magnitude(
            self,
            magnitude: Union[np.ndarray, int, float] = None,
            allow_reshape: bool = True,
            allow_transpose: bool = False,
            coords: list[str] = None,
            chunks: Union[tuple, str] = None,
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
            direction: Union[np.ndarray, int, float] = None,
            dir_type: str = None,
            allow_reshape: bool = True,
            allow_transpose: bool = False,
            coords: list[str] = None,
            chunks: Union[tuple, str] = None,
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

        if c._coord_manager.initial_state:
            c._coord_manager = deepcopy(c._coord_manager)
            c._coord_manager.initial_state = False

        name_str, meta = gp.decode(name)
        if direction is not None:
            dir_str, meta_dir = gp.decode(direction)
        else:
            dir_str, meta_dir = None, None

        c._coord_manager.add_magnitude(name_str, meta, x=x, y=y, dir=dir_str)

        if direction is not None:
            c._coord_manager.add_direction(
                name=dir_str,
                meta=meta_dir,
                x=x,
                y=y,
                dir_type=dir_type,
                mag=name_str,
            )
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

        return c

    if dir_type not in ["from", "to", "math", None]:
        raise ValueError(
            f"'dir_type' should be 'from', 'to' or 'math' (or 'None'), not {dir_type}!"
        )

    # Always respect explicitly set directional convention
    # Otherwise parse from MetaParameter is possible
    if dir_type is None:
        if gp.is_gp(direction):
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
