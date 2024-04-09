import numpy as np
from typing import Union
from copy import deepcopy
from functools import partial
import dask.array as da
from geo_parameters.metaparameter import MetaParameter


def add_magnitude(
    name,
    x: str,
    y: str,
    direction: str = None,
    meta: MetaParameter = None,
    meta_direction: MetaParameter = None,
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
            xvar = self._coord_manager.magnitudes.get(name)["x"]
            yvar = self._coord_manager.magnitudes.get(name)["y"]
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
                dirs = 90 - dirs * 180 / np.pi
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
            xvar = self._coord_manager.magnitudes.get(name)["x"]
            yvar = self._coord_manager.magnitudes.get(name)["y"]
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

        if c._coord_manager.initial_state:
            c._coord_manager = deepcopy(c._coord_manager)
            c._coord_manager.initial_state = False

        c._coord_manager.add_magnitude(name, x=x, y=y, meta=meta)

        if append:
            exec(f"c.{name} = partial(get_magnitude, c)")
        else:
            exec(f"c.{name} = get_magnitude")

        if direction is not None:
            c._coord_manager.add_direction(direction, x=x, y=y, meta=meta_direction)
            if append:
                exec(f"c.{direction} = partial(get_direction, c)")
            else:
                exec(f"c.{direction} = get_direction")

        return c

    return magnitude_decorator
