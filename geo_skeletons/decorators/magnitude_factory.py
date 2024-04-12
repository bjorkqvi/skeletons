import numpy as np
from typing import Union
from copy import deepcopy
from functools import partial
import dask.array as da
from geo_parameters.metaparameter import MetaParameter
from ..managers.dask_manager import DaskManager


def add_magnitude(
    name: Union[str, MetaParameter],
    x: str,
    y: str,
    direction: Union[str, MetaParameter] = None,
    direction_from: bool = None,
    append=False,
):
    """stash_get = True means that the coordinate data can be accessed
    by method ._{name}() instead of .{name}()

    This allows for alternative definitions of the get-method elsewere."""

    def magnitude_decorator(c):
        def get_direction(
            self,
            empty: bool = False,
            data_array: bool = False,
            squeeze: bool = False,
            dask: bool = None,
            angular: bool = False,
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

            if dask:
                dirs = da.arctan2(y, x)
            else:
                dirs = np.arctan2(y, x)

            if not angular:
                dirs = 90 - dirs * 180 / np.pi + offset
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
            direction: Union[np.ndarray, int, float] = None,
            angular: bool = False,
            allow_reshape: bool = True,
            allow_transpose: bool = False,
            coords: list[str] = None,
            chunks: Union[tuple, str] = None,
            silent: bool = True,
        ):
            if magnitude is None and direction is None:
                raise ValueError("magnitude and direction cannot both be None!")

            dask_manager = DaskManager(chunks=chunks or self.chunks or "auto")

            if magnitude is None:
                magnitude = eval(f"self.{name_str}()")
            else:
                magnitude = dask_manager.constant_array(
                    magnitude,
                    self.shape(name_str),
                    dask=(self.dask or chunks is not None),
                )
            if direction is None:
                direction = get_direction(
                    self, angular=angular
                )  # eval(f"self.{dir_str}(angular={angular})")
            else:
                direction = dask_manager.constant_array(
                    direction,
                    self.shape(name_str),
                    dask=(self.dask or chunks is not None),
                )
            if direction is None:
                raise ValueError("Cannot set x- and y-components without a direction!")

            if not angular:  # Convert to mathematical convention
                direction = (90 - direction + offset) * np.pi / 180

            if dask_manager.data_is_dask(direction):
                s = da.sin(direction)
                c = da.cos(direction)
            else:
                s = np.sin(direction)
                c = np.cos(direction)
            ux = magnitude * c
            uy = magnitude * s

            self.set(
                name=x,
                data=ux,
                allow_reshape=allow_reshape,
                allow_transpose=allow_transpose,
                coords=coords,
                chunks=chunks,
                silent=silent,
            )
            self.set(
                name=y,
                data=uy,
                allow_reshape=allow_reshape,
                allow_transpose=allow_transpose,
                coords=coords,
                chunks=chunks,
                silent=silent,
            )

        if c._coord_manager.initial_state:
            c._coord_manager = deepcopy(c._coord_manager)
            c._coord_manager.initial_state = False

        name_str = c._coord_manager.add_magnitude(name, x=x, y=y)

        if append:
            exec(f"c.{name_str} = partial(get_magnitude, c)")
            exec(f"c.set_{name_str} = partial(set_magnitude, c)")
        else:
            exec(f"c.{name_str} = get_magnitude")
            exec(f"c.set_{name_str} = set_magnitude")

        if direction is not None:
            dir_str = c._coord_manager.add_direction(direction, x=x, y=y)
            if append:
                exec(f"c.{dir_str} = partial(get_direction, c)")
            else:
                exec(f"c.{dir_str} = get_direction")

        return c

    # Always respect explicitly set directional convention
    # Otherwise parse from MetaParameter is possible
    # Default to direction_from
    if direction_from is None:
        if not isinstance(direction, str):
            direction_from = not (
                "to_direction" in direction.standard_name()
                or "to_direction" in direction.standard_name(alias=True)
            )
        else:
            direction_from = True
    offset = 180 if direction_from else 0
    return magnitude_decorator
