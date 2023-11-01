import numpy as np
from .coordinate_manager import CoordinateManager

CARTESIAN_STRINGS = ["x", "y", "xy"]
SPHERICAL_STRINGS = ["lon", "lat", "lonlat"]


def add_mask(
    name: str,
    default_value: int,
    coords: str = "grid",
    opposite_name: str = None,
):
    """coord_type = 'all', 'spatial', 'grid' or 'gridpoint'"""

    def mask_decorator(c):
        def get_mask(self, empty: bool = False, **kwargs) -> np.ndarray:
            """Returns bool array of the mask.

            Set boolean=False to get 0 for land and 1 for sea.
            Set empty=True to get an empty mask (even if it doesn't exist)

            **kwargs can be used for slicing data.
            """

            mask = self.get(f"{name}_mask", boolean_mask=True, **kwargs)

            return mask

        def get_not_mask(self, **kwargs):
            mask = get_mask(self, **kwargs)
            if mask is None:
                return None
            return np.logical_not(mask)

        def get_masked_points(
            self,
            type: str = "lonlat",
            native: bool = True,
            order_by: str = "lat",
            strict=False,
            **kwargs,
        ):
            mask = get_mask(self, **kwargs)

            if type in CARTESIAN_STRINGS:
                return self.xy(
                    mask=mask, native=native, order_by=order_by, strict=strict, **kwargs
                )
            elif type in SPHERICAL_STRINGS:
                return self.lonlat(
                    mask=mask, native=native, order_by=order_by, strict=strict, **kwargs
                )

        def get_not_points(
            self,
            type: str = "lonlat",
            native: bool = True,
            order_by: str = "lat",
            strict=False,
            **kwargs,
        ):
            mask = np.logical_not(get_mask(self, **kwargs))

            if type in CARTESIAN_STRINGS:
                return self.xy(
                    mask=mask, native=native, order_by=order_by, strict=strict, **kwargs
                )
            elif type in SPHERICAL_STRINGS:
                return self.lonlat(
                    mask=mask, native=native, order_by=order_by, strict=strict, **kwargs
                )

        def set_mask(self, data: np.ndarray = None) -> None:
            self.set(f"{name}_mask", data.astype(bool))

        def set_opposite_mask(self, data: np.ndarray = None) -> None:
            self.set(f"{name}_mask", np.logical_not(data))

        if not hasattr(c, "_coord_manager"):
            c._coord_manager = CoordinateManager()
        c._coord_manager.add_mask(name, coords, default_value)
        exec(f"c.{name}_mask = get_mask")
        exec(f"c.{name}_points = get_masked_points")
        exec(f"c.set_{name}_mask = set_mask")
        if opposite_name is not None:
            exec(f"c.{opposite_name}_mask = get_not_mask")
            exec(f"c.{opposite_name}_points = get_not_points")
            exec(f"c.set_{opposite_name}_mask = set_opposite_mask")

        return c

    return mask_decorator
