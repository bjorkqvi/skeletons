import numpy as np
from .skeleton import Skeleton
import xarray as xr

class PointSkeleton(Skeleton):
    """Gives a unstructured structure to the Skeleton.

    In practise this means that:

    1) Grid coordinates are defined with and index (inds),
    2) x,y / lon,lat values are data variables of the index.
    3) Methods x(), y() / lon(), lat() will returns all points.
    4) Methods xy() / lonlat() are identical to e.g. (x(), y()).
    """

    @classmethod
    def from_skeleton(
        cls,
        skeleton: Skeleton,
        mask: np.ndarray = None,
    ):
        
        if mask is None:
            mask = np.full(skeleton.size('spatial'), True)

        lon, lat = skeleton.lonlat(strict=True, mask=mask)
        x, y = skeleton.xy(strict=True, mask=mask)

        new_skeleton = cls(lon=lon, lat=lat, x=x, y=y, name=skeleton.name)
        new_skeleton.set_utm(skeleton.utm(), silent=True)

        return new_skeleton
    
    def is_gridded(self) -> bool:
        return False

    def _initial_coords(self) -> list[str]:
        """Initial coordinates used with PointSkeletons. Additional coordinates
        can be added by decorators (e.g. @add_time).
        """
        return ["inds"]

    def _initial_vars(self) -> dict:
        """Initial variables used with PointSkeletons. Additional variables
        can be added by decorator @add_datavar.
        """
        return {"x": "inds", "y": "inds"}

    def lonlat(
        self,
        mask: np.ndarray = None,
        native: bool = False,
        strict: bool = False,
        **kwargs,
    ) -> tuple[np.ndarray, np.ndarray]:
        """Returns a tuple of longitude and latitude of all points.

        If native=True, then x-y coordinatites are returned for cartesian grids instead
        If strict=True, then (None, None) is returned if grid is cartesian

        native=True overrides strict=True for cartesian grids

        Identical to (.lon(), .lat()) (with no mask)
        mask is a boolean array (default True for all points)
        """

        lon, lat = super().lon(native=native, strict=strict, **kwargs), super().lat(
            native=native, strict=strict, **kwargs
        )

        if lon is None:
            return None, None
        if mask is not None:
            return lon[mask], lat[mask]
        return lon, lat

    def xy(
        self, mask: np.ndarray = None, strict=False, normalize: bool = False, utm: tuple[int, str]=None, **kwargs
    ) -> tuple[np.ndarray, np.ndarray]:
        """Returns a tuple of x- and y-coordinates of all points.

        If native=True, then lon-lat coordinatites are returned for spherical grids instead
        If strict=True, then (None, None) is returned if grid is spherical

        native=True overrides strict=True for spherical grids

        Identical to (.x(), .y()) (with no mask)
        mask is a boolean array (default True for all points)
        """

        # Transforms x-y to lon-lat if necessary
        x, y = super().x(strict=strict, normalize=normalize, utm=utm, **kwargs), super().y(
            strict=strict, normalize=normalize, utm=utm, **kwargs
        )

        if x is None:
            return None, None

        if mask is not None:
            return x[mask], y[mask]
        return x, y

    def __repr__(self) -> str:
        string = f"<{type(self).__name__} (PointSkeleton)>\n"
        string += "-"*34 + " Containing " + "-"*34 + "\n"
        string += self.ds().__repr__()
        string += "\n" + "-"*80
        return string

