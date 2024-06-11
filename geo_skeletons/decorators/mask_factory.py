import numpy as np
from copy import deepcopy

CARTESIAN_STRINGS = ["x", "y", "xy"]
SPHERICAL_STRINGS = ["lon", "lat", "lonlat"]

from typing import Union, Optional
import dask
from geo_parameters.metaparameter import MetaParameter

from ..managers.dask_manager import DaskManager

import geo_parameters as gp
from geo_skeletons.variables import GridMask


def add_mask(
    name: Union[str, MetaParameter],
    default_value: int = 0,
    coord_group: str = "grid",
    opposite_name: Optional[Union[str, MetaParameter]] = None,
    triggered_by: Optional[str] = None,
    valid_range: tuple[float] = (0.0, None),
    range_inclusive: bool = True,
):
    """coord_type = 'all', 'spatial', 'grid' or 'gridpoint'"""

    def mask_decorator(c):
        def get_mask(self, empty: bool = False, **kwargs) -> np.ndarray:
            """Returns bool array of the mask.

            Set empty=True to get an empty mask (even if it doesn't exist)

            **kwargs can be used for slicing data.
            """

            mask = self.get(
                f"{name_str}_mask", boolean_mask=True, empty=empty, **kwargs
            )

            return mask

        def get_not_mask(self, empty: bool = False, **kwargs):
            mask = get_mask(self, empty=empty, **kwargs)
            if mask is None:
                return None
            return np.logical_not(mask)

        def get_masked_points(
            self,
            coord: Optional[str] = None,
            native: bool = False,
            strict: bool = False,
            **kwargs,
        ):
            mask = get_mask(self, **kwargs)
            if mask is None:
                mask = get_mask(self, empty=True, **kwargs)

            coord = coord or self.core.x_str

            if coord in CARTESIAN_STRINGS:
                return self.xy(mask=mask, native=native, strict=strict, **kwargs)
            elif coord in SPHERICAL_STRINGS:
                return self.lonlat(mask=mask, native=native, strict=strict, **kwargs)

        def get_not_points(
            self,
            coord: Optional[str] = None,
            native: bool = False,
            strict: bool = False,
            **kwargs,
        ):
            mask = get_not_mask(self, **kwargs)
            if mask is None:
                mask = get_not_mask(self, empty=True, **kwargs)

            coord = coord or self.core.x_str

            if coord in CARTESIAN_STRINGS:
                return self.xy(mask=mask, native=native, strict=strict, **kwargs)
            elif coord in SPHERICAL_STRINGS:
                return self.lonlat(mask=mask, native=native, strict=strict, **kwargs)

        def set_mask(
            self,
            data: Optional[Union[np.ndarray, int, bool]] = None,
            allow_reshape: bool = True,
            allow_transpose: bool = False,
            coords: Optional[list[str]] = None,
            chunks: Optional[Union[tuple, str]] = None,
            silent: bool = True,
        ) -> None:
            self.set(
                f"{name_str}_mask",
                data,
                allow_reshape=allow_reshape,
                allow_transpose=allow_transpose,
                coords=coords,
                chunks=chunks,
                silent=silent,
            )

        def set_opposite_mask(
            self,
            data: Optional[Union[np.ndarray, int, bool]] = None,
            allow_reshape: bool = True,
            allow_transpose: bool = False,
            coords: Optional[list[str]] = None,
            chunks: Optional[Union[tuple, str]] = None,
            silent: bool = True,
        ) -> None:

            data = self.dask.dask_me(data)

            if data is not None:
                if self.dask.is_active() or chunks is not None:
                    data = dask.array.logical_not(data)
                else:
                    data = np.logical_not(data)

            self.set(
                f"{name_str}_mask",
                data,
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
        if opposite_name is not None:
            opposite_name_str, opposite_meta = gp.decode(opposite_name)
            opposite_grid_mask = GridMask(
                name=f"{opposite_name_str}_mask",
                meta=opposite_meta,
                coord_group=coord_group,
                primary_mask=False,
            )
        else:
            opposite_name_str = None
            opposite_grid_mask = None

        grid_mask = GridMask(
            name=f"{name_str}_mask",
            meta=meta,
            coord_group=coord_group,
            default_value=default_value,
            primary_mask=True,
            opposite_mask=opposite_grid_mask,
            triggered_by=triggered_by,
            valid_range=valid_range,
            range_inclusive=range_inclusive,
        )

        c.core.add_mask(grid_mask)
        if opposite_grid_mask is not None:
            c.core.add_mask(opposite_grid_mask)

        exec(f"c.{name_str}_mask = get_mask")
        exec(f"c.{name_str}_points = get_masked_points")
        exec(f"c.set_{name_str}_mask = set_mask")
        if opposite_name_str is not None:
            exec(f"c.{opposite_name_str}_mask = get_not_mask")
            exec(f"c.{opposite_name_str}_points = get_not_points")
            exec(f"c.set_{opposite_name_str}_mask = set_opposite_mask")

        return c

    return mask_decorator
