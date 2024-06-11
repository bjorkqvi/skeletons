from __future__ import annotations
from typing import TYPE_CHECKING
import numpy as np
from .skeleton import Skeleton
from .managers.coordinate_manager import CoordinateManager
from .managers.metadata_manager import MetaDataManager
from .managers.dask_manager import DaskManager
from .variables import DataVar, Coordinate
import geo_parameters as gp
from typing import Optional
from .dask_computations import undask_me

inds_coord = Coordinate(name="inds", meta=gp.grid.Inds, coord_group="spatial")
INITIAL_COORDS = [inds_coord]

lon_var = DataVar(name="lon", meta=gp.grid.Lon, coord_group="spatial", default_value=0)
lat_var = DataVar(name="lat", meta=gp.grid.Lat, coord_group="spatial", default_value=0)
x_var = DataVar(name="x", meta=gp.grid.X, coord_group="spatial", default_value=0)
y_var = DataVar(name="y", meta=gp.grid.Y, coord_group="spatial", default_value=0)
INITIAL_CARTESIAN_VARS = [x_var, y_var]  #: "inds", "y": "inds"}
INITIAL_SPHERICAL_VARS = [lon_var, lat_var]  # {"lat": "inds", "lon": "inds"}


class PointSkeleton(Skeleton):
    """Gives a unstructured structure to the Skeleton.

    In practise this means that:

    1) Grid coordinates are defined with and index (inds),
    2) x,y / lon,lat values are data variables of the index.
    3) Methods x(), y() / lon(), lat() will returns all points.
    4) Methods xy() / lonlat() are identical to e.g. (x(), y()).
    """

    core = CoordinateManager(INITIAL_COORDS, INITIAL_CARTESIAN_VARS)
    meta = MetaDataManager(ds_manager=None, coord_manager=core)

    @classmethod
    def from_skeleton(
        cls,
        skeleton: Skeleton,
        mask: Optional[np.ndarray] = None,
    ) -> PointSkeleton:
        """Creates a new PointSkeleton containing only points from another Gridded- or PointSkeleton.

        Points can be selected by a boolean mask. No data is transferred"""

        if mask is None:
            mask = np.full(skeleton.size("spatial"), True)
        mask = undask_me(mask)
        lon, lat = skeleton.lonlat(strict=True, mask=mask)
        x, y = skeleton.xy(strict=True, mask=mask)

        new_skeleton = cls(lon=lon, lat=lat, x=x, y=y, name=skeleton.name)
        new_skeleton.utm.set(skeleton.utm.zone(), silent=True)

        return new_skeleton

    def is_gridded(self) -> bool:
        return False

    @staticmethod
    def _initial_coords(spherical: bool = False) -> list[Coordinate]:
        """Initial coordinates used with PointSkeletons. Additional coordinates
        can be added by decorators (e.g. @add_coord, @add_time).
        """
        return INITIAL_COORDS

    @staticmethod
    def _initial_vars(spherical: bool = False) -> list[DataVar]:
        """Initial variables used with PointSkeletons. Additional variables
        can be added by decorator @add_datavar.
        """
        if spherical:
            return INITIAL_SPHERICAL_VARS
        else:
            return INITIAL_CARTESIAN_VARS

    def xgrid(
        self,
        native: bool = False,
        strict: bool = False,
        mask: Optional[np.ndarray] = None,
        normalize: bool = False,
    ) -> np.ndarray:
        """Gives a meshgrid of UTM x-values.

        NB! Identical to Skeleton.lat() since PointSkeletons are not gridded!

        strict = True gives 'None' if Skeleton is spherical
        native = True gives longitude values if Skeleton is spherical"""
        x, _ = self.xy(native=native, strict=strict, normalize=normalize, mask=mask)
        return x

    def ygrid(
        self,
        native: bool = False,
        strict: bool = False,
        mask: Optional[np.ndarray] = None,
        normalize: bool = False,
    ) -> np.ndarray:
        """Gives a meshgrid of UTM x-values.

        NB! Identical to Skeleton.lat() since PointSkeletons are not gridded!

        strict = True gives 'None' if Skeleton is spherical
        native = True gives longitude values if Skeleton is spherical"""
        _, y = self.xy(native=native, strict=strict, normalize=normalize, mask=mask)
        return y

    def longrid(
        self,
        native: bool = False,
        strict: bool = False,
        mask: Optional[np.ndarray] = None,
    ) -> np.ndarray:
        """Gives a meshgrid of longitude values. 'None' for cartesian grids that have no UTM-zone.

        NB! Identical to Skeleton.lat() since PointSkeletons are not gridded!

        strict = True gives 'None' if Skeleton is cartesian
        native = True gives UTM x-values if Skeleton is cartesian"""
        lon, _ = self.lonlat(native=native, strict=strict, mask=mask)
        return lon

    def latgrid(
        self,
        native: bool = False,
        strict: bool = False,
        mask: Optional[np.ndarray] = None,
    ) -> np.ndarray:
        """Gives a meshgrid of latitude values. 'None' for cartesian grids that have no UTM-zone.

        NB! Identical to Skeleton.lat() since PointSkeletons are not gridded!

        strict = True gives 'None' if Skeleton is cartesian
        native = True gives UTM y-values if Skeleton is cartesian"""
        _, lat = self.lonlat(native=native, strict=strict, mask=mask)
        return lat

    def x(
        self,
        native: bool = False,
        strict: bool = False,
        utm: Optional[tuple[int, str]] = None,
        mask: Optional[np.ndarray] = None,
        normalize: bool = False,
        **kwargs,
    ) -> np.ndarray:
        """Returns the cartesian x-coordinate.

        strict = True gives 'None' if Skeleton is spherical
        native = True gives longitude values if Skeleton is spherical

        Give 'utm' to get cartesian coordinates in specific UTM-zone. Otherwise defaults to the one set for the grid.
        """

        mask = self._check_mask_right_shape(mask, **kwargs)
        if native and strict:
            raise ValueError("Can't set both 'native' and 'strict' to True!")

        if self.ds() is None:
            return None

        if not self.core.is_cartesian() and native:
            return self.lon(mask=mask, **kwargs)

        if not self.core.is_cartesian() and strict:
            return None

        if self.core.is_cartesian() and (self.utm.zone() == utm or utm is None):
            x = self._ds_manager.get("x", **kwargs).values.copy()[mask]
        else:
            x = self.utm._x(
                lon=self.lon(mask=mask, **kwargs),
                lat=self.lat(mask=mask, **kwargs),
                utm=utm,
            )

        if normalize:
            x = x - min(x)

        return x

    def y(
        self,
        native: bool = False,
        strict: bool = False,
        mask: Optional[np.ndarray] = None,
        utm: Optional[tuple[int, str]] = None,
        normalize: bool = False,
        **kwargs,
    ) -> np.ndarray:
        """Returns the cartesian y-coordinate.

        strict = True gives 'None' if Skeleton is spherical
        native = True gives latitude values if Skeleton is spherical

        Give 'utm' to get cartesian coordinates in specific UTM-zone. Otherwise defaults to the one set for the grid.
        """

        mask = self._check_mask_right_shape(mask, **kwargs)

        if native and strict:
            raise ValueError("Can't set both 'native' and 'strict' to True!")
        if self.ds() is None:
            return None

        if not self.core.is_cartesian() and native:
            return self.lat(mask=mask, **kwargs)

        if not self.core.is_cartesian() and strict:
            return None

        utm = utm or self.utm.zone()

        if self.core.is_cartesian() and (self.utm.zone() == utm):
            y = self._ds_manager.get("y", **kwargs).values.copy()[mask]
        else:
            y = self.utm._y(
                lon=self.lon(mask=mask, **kwargs),
                lat=self.lat(mask=mask, **kwargs),
                utm=utm,
            )

        if normalize:
            y = y - min(y)

        return y

    def lon(
        self,
        native: bool = False,
        strict=False,
        mask: Optional[np.ndarray] = None,
        **kwargs,
    ) -> np.ndarray:
        """Returns the spherical lon-coordinate. 'None' for cartesian grids that have no UTM-zone.

        strict = True gives 'None' if Skeleton is cartesian
        native = True gives UTM x-values if Skeleton is cartesian
        """

        mask = self._check_mask_right_shape(mask, **kwargs)

        if native and strict:
            raise ValueError("Can't set both 'native' and 'strict' to True!")

        if self.ds() is None:
            return None

        if self.core.is_cartesian() and native:
            return self.x(mask=mask, **kwargs)

        if self.core.is_cartesian() and strict:
            return None

        if not self.core.is_cartesian():
            return self._ds_manager.get("lon", **kwargs).values.copy()[mask]

        return self.utm._lon(
            x=self.x(mask=mask, **kwargs), y=self.y(mask=mask, **kwargs)
        )

    def lat(
        self,
        native: bool = False,
        strict=False,
        mask: Optional[np.ndarray] = None,
        **kwargs,
    ) -> np.ndarray:
        """Returns the spherical lat-coordinate. 'None' for cartesian grids that have no UTM-zone.

        strict = True gives 'None' if Skeleton is cartesian
        native = True gives UTM x-values if Skeleton is cartesian
        """

        mask = self._check_mask_right_shape(mask, **kwargs)

        if native and strict:
            raise ValueError("Can't set both 'native' and 'strict' to True!")

        if self.ds() is None:
            return None

        if self.core.is_cartesian() and native:
            return self.y(mask=mask, **kwargs)

        if self.core.is_cartesian() and strict:
            return None

        if not self.core.is_cartesian():
            return self._ds_manager.get("lat", **kwargs).values.copy()[mask]

        return self.utm._lat(
            x=self.x(mask=mask, **kwargs), y=self.y(mask=mask, **kwargs)
        )

    def xy(
        self,
        strict: bool = False,
        mask: Optional[np.ndarray] = None,
        utm: Optional[tuple[int, str]] = None,
        normalize: bool = False,
        **kwargs,
    ) -> tuple[np.ndarray, np.ndarray]:
        """Returns a tuple of UTM x- and y-coordinates of all points.

        strict = True gives '(None, None)' if Skeleton is spherical
        native = True gives UTM longitude,latitude-values if Skeleton is spherical

        Give 'utm' to get cartesian coordinates in specific UTM-zone. Otherwise defaults to the one set for the grid.

        Identical to (.x(), .y()) (with no mask)
        mask is a boolean array (default True for all points)
        """

        mask = self._check_mask_right_shape(mask, **kwargs)

        # Transforms x-y to lon-lat if necessary
        x, y = self.x(
            strict=strict, normalize=normalize, utm=utm, mask=mask, **kwargs
        ), self.y(strict=strict, normalize=normalize, utm=utm, mask=mask, **kwargs)

        if x is None:
            return None, None

        return x, y

    def lonlat(
        self,
        native: bool = False,
        strict: bool = False,
        mask: Optional[np.ndarray] = None,
        **kwargs,
    ) -> tuple[np.ndarray, np.ndarray]:
        """Returns a tuple of longitude and latitude of all points.

        strict = True gives '(None, None)' if Skeleton is cartesian
        native = True gives UTM x,y-values if Skeleton is cartesian

        Identical to (.lon(), .lat()) (with no mask)
        mask is a boolean array (default True for all points)
        """

        mask = self._check_mask_right_shape(mask)

        lon, lat = self.lon(
            native=native, strict=strict, mask=mask, **kwargs
        ), self.lat(native=native, strict=strict, mask=mask, **kwargs)

        if lon is None:
            return None, None
        return lon, lat

    def _check_mask_right_shape(self, mask: np.ndarray, **kwargs) -> np.array:
        """Checks that the given mask is same shape as the skeleton.
        Creates a full True maks if mask is None"""
        if mask is None:
            return np.full(self.size("spatial", **kwargs), True)
        mask = np.array(mask)

        if mask.shape != self.size("spatial", **kwargs):
            raise ValueError(
                f"Skeleton has shape {self.size('spatial',**kwargs)} but mask is shape {mask.shape}"
            )
        return mask
