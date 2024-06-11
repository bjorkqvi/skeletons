from __future__ import annotations
from typing import TYPE_CHECKING
import numpy as np
from .skeleton import Skeleton
from .point_skeleton import PointSkeleton
from . import distance_funcs
from .managers.coordinate_manager import CoordinateManager
from .managers.dask_manager import DaskManager
from .managers.metadata_manager import MetaDataManager
from .variables import Coordinate, DataVar
import geo_parameters as gp
from typing import Optional
from .dask_computations import undask_me

lon_var = Coordinate(name="lon", meta=gp.grid.Lon, coord_group="spatial")
lat_var = Coordinate(name="lat", meta=gp.grid.Lat, coord_group="spatial")
x_var = Coordinate(name="x", meta=gp.grid.X, coord_group="spatial")
y_var = Coordinate(name="y", meta=gp.grid.Y, coord_group="spatial")

INITIAL_CARTESIAN_COORDS = [y_var, x_var]
INITIAL_SPERICAL_COORDS = [lat_var, lon_var]

INITIAL_VARS = []


class GriddedSkeleton(Skeleton):
    """Gives a gridded structure to the Skeleton.

    In practise this means that:

    1) Grid coordinates are defined as x,y / lon,lat.
    2) Methods x(), y() / lon(), lat() will return the vectors defining the grid.
    3) Methods xy() / lonlat() will return a list of all points of the grid
    (i.e. raveled meshgrid).
    """

    core = CoordinateManager(INITIAL_CARTESIAN_COORDS, INITIAL_VARS)
    meta = MetaDataManager(ds_manager=None, coord_manager=core)

    @classmethod
    def from_skeleton(
        cls,
        skeleton: Skeleton,
        mask: Optional[np.ndarray] = None,
    ) -> GriddedSkeleton:
        """Creates a new PointSkeleton containing only points from another GriddedSkeleton.

        Points can be selected by a boolean mask. No data is transferred"""
        if not skeleton.is_gridded():
            raise TypeError(
                "Can't create a GriddedSkeleton from a non-gridded data structure!"
            )

        if mask is None:
            mask = np.full(skeleton.size("spatial"), True)
        mask = undask_me(mask)

        lon, lat = skeleton.lon(strict=True, mask=mask), skeleton.lat(
            strict=True, mask=mask
        )
        x, y = skeleton.x(strict=True, mask=mask), skeleton.y(strict=True, mask=mask)

        new_skeleton = cls(lon=lon, lat=lat, x=x, y=y, name=skeleton.name)
        new_skeleton.utm.set(skeleton.utm.zone(), silent=True)

        return new_skeleton

    def is_gridded(self) -> bool:
        return True

    @staticmethod
    def _initial_coords(spherical: bool = False) -> list[Coordinate]:
        """Initial coordinates used with GriddedSkeletons. Additional coordinates
        can be added by decorators (e.g. @add_coord, @add_time).
        """
        if spherical:
            return INITIAL_SPERICAL_COORDS
        else:
            return INITIAL_CARTESIAN_COORDS

    @staticmethod
    def _initial_vars(spherical: bool = False) -> list[DataVar]:
        """Initial coordinates used with GriddedSkeletons. Additional variables
        can be added by decorator @add_datavar.
        """
        return INITIAL_VARS

    def xgrid(
        self, native: bool = False, strict: bool = False, normalize: bool = False
    ) -> np.ndarray:
        """Gives a meshgrid of UTM x-values.

        strict = True gives 'None' if Skeleton is spherical
        native = True gives longitude values if Skeleton is spherical"""
        if not self.core.is_cartesian() and strict:
            return None
        x, _ = self.xy(native=native, normalize=normalize)
        return np.reshape(x, self.size("spatial"))

    def ygrid(
        self, native: bool = False, strict: bool = False, normalize: bool = False
    ) -> np.ndarray:
        """Gives a meshgrid of UTM y-values.

        strict = True gives 'None' if Skeleton is spherical
        native = True gives longitude values if Skeleton is spherical"""
        if not self.core.is_cartesian() and strict:
            return None
        _, y = self.xy(native=native, normalize=normalize)
        return np.reshape(y, self.size("spatial"))

    def longrid(self, native: bool = False, strict: bool = False) -> np.ndarray:
        """Gives a meshgrid of longitude values. 'None' for cartesian grids that have no UTM-zone.

        strict = True gives 'None' if Skeleton is cartesian
        native = True gives UTM x-values if Skeleton is cartesian"""
        if self.core.is_cartesian() and strict:
            return None
        lon, _ = self.lonlat(native=native)
        if lon is None:  # Might happen if UTM-zone is not set
            return None
        return np.reshape(lon, self.size("spatial"))

    def latgrid(self, native: bool = False, strict: bool = False) -> np.ndarray:
        """Gives a meshgrid of latitude values. 'None' for cartesian grids that have no UTM-zone.

        strict = True gives 'None' if Skeleton is cartesian
        native = True gives UTM y-values if Skeleton is cartesian"""
        if self.core.is_cartesian() and strict:
            return None
        _, lat = self.lonlat(native=native)

        if lat is None:  # Might happen if UTM-zone is not set
            return None
        return np.reshape(lat, self.size("spatial"))

    def x(
        self,
        native: bool = False,
        strict: bool = False,
        mask: Optional[np.ndarray] = None,
        normalize: bool = False,
        utm: tuple[int, str] = None,
        suppress_warning: bool = False,
        **kwargs,
    ) -> np.ndarray:
        """Returns the cartesian x-coordinate.

        If the grid is spherical, a conversion to UTM coordinates is made based on the median latitude.

        strict = True gives 'None' if Skeleton is spherical
        native = True gives longitude values if Skeleton is spherical

        Give 'utm' to get cartesian coordinates in specific UTM-zone. Otherwise defaults to the one set for the grid.
        """

        mask = self._check_mask_right_shape(mask, self.core.x_str, **kwargs)
        vec_mask = np.any(mask, axis=0)
        if native and strict:
            raise ValueError("Can't set both 'native' and 'strict' to True!")
        if self.ds() is None:
            return None

        if not self.core.is_cartesian() and native:
            return self.lon(**kwargs)

        if not self.core.is_cartesian() and strict:
            return None

        if self.core.is_cartesian() and (self.utm.zone() == utm or utm is None):
            x = self._ds_manager.get("x", **kwargs).values.copy()[vec_mask]
        else:
            lon, lat = self.lon(mask=mask, **kwargs), self.lat(mask=mask, **kwargs)
            median_lat = np.full(len(lon), np.median(lat))
            if not suppress_warning and len(lat) > 1:
                print(
                    "Regridding spherical grid to cartesian coordinates will cause a rotation! Use '_, y = skeleton.xy()' to get a list of all points."
                )
            x = self.utm._x(lon=lon, lat=median_lat, utm=utm)

        if normalize:
            x = x - min(x)
        return x

    def y(
        self,
        native: bool = False,
        strict: bool = False,
        mask: Optional[np.ndarray] = None,
        normalize: bool = False,
        utm: tuple[int, str] = None,
        suppress_warning: bool = False,
        **kwargs,
    ) -> np.ndarray:
        """Returns the cartesian y-coordinate.

        If the grid is spherical, a conversion to UTM coordinates is made based on the median latitude.

        strict = True gives 'None' if Skeleton is spherical
        native = True gives latitude values if Skeleton is spherical

        Give 'utm' to get cartesian coordinates in specific UTM-zone. Otherwise defaults to the one set for the grid.
        """

        mask = self._check_mask_right_shape(mask, self.core.y_str, **kwargs)
        vec_mask = np.any(mask, axis=1)
        if native and strict:
            raise ValueError("Can't set both 'native' and 'strict' to True!")
        if self.ds() is None:
            return None

        if not self.core.is_cartesian() and native:
            return self.lat(**kwargs)

        if not self.core.is_cartesian() and strict:
            return None

        if self.core.is_cartesian() and (self.utm.zone() == utm or utm is None):
            y = self._ds_manager.get("y", **kwargs).values.copy()[vec_mask]
        else:
            lon, lat = self.lon(mask=mask, **kwargs), self.lat(mask=mask, **kwargs)
            median_lon = np.full(len(lat), np.median(lon))
            if not suppress_warning and len(lon) > 1:
                print(
                    "Regridding spherical grid to cartesian coordinates will cause a rotation! Use 'x, _ = skeleton.xy()' to get a list of all points."
                )
            y = self.utm._y(lon=median_lon, lat=lat, utm=utm)

        if normalize:
            y = y - min(y)

        return y

    def lon(
        self,
        native: bool = False,
        strict=False,
        mask: Optional[np.ndarray] = None,
        suppress_warning: bool = False,
        **kwargs,
    ) -> np.ndarray:
        """Returns the spherical lon-coordinate. 'None' for cartesian grids that have no UTM-zone.

        If the grid is cartesian, a conversion from UTM coordinates is made based on the median y-coordinate.

        strict = True gives 'None' if Skeleton is cartesian
        native = True gives UTM x-values if Skeleton is cartesian
        """

        mask = self._check_mask_right_shape(mask, self.core.x_str, **kwargs)
        vec_mask = np.any(mask, axis=0)
        if native and strict:
            raise ValueError("Can't set both 'native' and 'strict' to True!")

        if self.ds() is None:
            return None

        if self.core.is_cartesian() and native:
            return self.x(**kwargs)

        if self.core.is_cartesian() and strict:
            return None

        if not self.core.is_cartesian():
            return self._ds_manager.get("lon", **kwargs).values.copy()[vec_mask]

        x, y = self.x(mask=mask, **kwargs), self.y(mask=mask, **kwargs)
        median_y = np.full(len(x), np.median(y))

        if not suppress_warning and len(y) > 1:
            print(
                "Regridding cartesian grid to spherical coordinates will cause a rotation! Use 'lon, _ = skeleton.lonlat()' to get a list of all points."
            )
        return self.utm._lon(x=x, y=median_y)

    def lat(
        self,
        native: bool = False,
        strict=False,
        mask: Optional[np.ndarray] = None,
        suppress_warning: bool = False,
        **kwargs,
    ) -> np.ndarray:
        """Returns the spherical lat-coordinate. 'None' for cartesian grids that have no UTM-zone.

        If the grid is cartesian, a conversion from UTM coordinates is made based on the median y-coordinate.

        strict = True gives 'None' if Skeleton is cartesian
        native = True gives UTM y-values if Skeleton is cartesian
        """

        mask = self._check_mask_right_shape(mask, self.core.y_str, **kwargs)
        vec_mask = np.any(mask, axis=1)
        if native and strict:
            raise ValueError("Can't set both 'native' and 'strict' to True!")

        if self.ds() is None:
            return None

        if self.core.is_cartesian() and native:
            return self.y(**kwargs)

        if self.core.is_cartesian() and strict:
            return None

        if not self.core.is_cartesian():
            return self._ds_manager.get("lat", **kwargs).values.copy()[vec_mask]

        x, y = self.x(mask=mask, **kwargs), self.y(mask=mask, **kwargs)
        median_x = np.full(len(y), np.median(x))
        if not suppress_warning and len(x) > 1:
            print(
                "Regridding cartesian grid to spherical coordinates will cause a rotation! Use '_, lat = skeleton.lonlat()' to get a list of all points."
            )

        return self.utm._lat(x=median_x, y=y)

    def xy(
        self,
        native: bool = False,
        strict: bool = False,
        mask: Optional[np.ndarray] = None,
        utm: tuple[int, str] = None,
        normalize: bool = False,
        **kwargs,
    ) -> tuple[np.ndarray, np.ndarray]:
        """Returns a tuple of UTM x- and y-coordinates of all points.

        strict = True gives '(None, None)' if Skeleton is spherical
        native = True gives UTM longitude,latitude-values if Skeleton is spherical

        Give 'utm' to get cartesian coordinates in specific UTM-zone. Otherwise defaults to the one set for the grid.

        mask is a boolean array (default True for all points)
        """

        if native and strict:
            raise ValueError("Can't set both 'native' and 'strict' to True!")
        if not self.core.is_cartesian() and strict:
            return None, None

        if mask is None:
            mask = np.full(super().size("spatial", **kwargs), True)

        num_of_elements = (
            self.shape(self.core.x_str)[0] * self.shape(self.core.y_str)[0]
        )
        if mask.ravel().shape[0] != num_of_elements:
            raise ValueError(
                f"Skeleton has {num_of_elements} elements but mask has shape {mask.shape}, not ({num_of_elements},)!"
            )
        mask = mask.ravel()

        x, y = self._native_xy(utm=utm, normalize=normalize, **kwargs)
        if self.core.is_cartesian() or native:
            return x[mask], y[mask]

        # Only convert if skeleton is not Cartesian and native output is not requested
        points = PointSkeleton(lon=x, lat=y)
        points.utm.set(self.utm.zone(), silent=True)

        return points.xy(mask=mask, normalize=normalize)

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

        mask is a boolean array (default True for all points)
        """

        if native and strict:
            raise ValueError("Can't set both 'native' and 'strict' to True!")

        if self.core.is_cartesian() and strict:
            return None, None

        if mask is None:
            mask = np.full(super().size("spatial", **kwargs), True)

        num_of_elements = (
            self.shape(self.core.x_str)[0] * self.shape(self.core.y_str)[0]
        )
        if mask.ravel().shape[0] != num_of_elements:
            raise ValueError(
                f"Skeleton has {num_of_elements} elements but mask has shape {mask.shape}, not ({num_of_elements},)!"
            )
        mask = mask.ravel()
        x, y = self._native_xy(**kwargs)

        if not self.core.is_cartesian() or native:
            return x[mask], y[mask]

        # Only convert if skeleton is Cartesian and native output is not requested
        points = PointSkeleton(x=x, y=y)
        points.utm.set(self.utm.zone(), silent=True)

        return points.lonlat(mask=mask)

    def _native_xy(
        self, utm: Optional[tuple[int, str]] = None, normalize: bool = False, **kwargs
    ) -> tuple[np.ndarray, np.ndarray]:
        """Returns a tuple of native x and y of all points."""

        x, y = np.meshgrid(
            self.x(native=True, utm=utm, normalize=normalize, **kwargs),
            self.y(native=True, utm=utm, normalize=normalize, **kwargs),
        )

        return x.ravel(), y.ravel()

    def set_spacing(
        self,
        dlon: float = 0.0,
        dlat: float = 0.0,
        dx: float = 0.0,
        dy: float = 0.0,
        dm: float = 0.0,
        dnmi: float = 0.0,
        nx: int = 0,
        ny: int = 0,
        floating_edge: bool = False,
    ) -> None:
        """Defines longitude and latitude vectors based on desired spacing.

        Options (priority in this order)
        nx, ny [grid points]:   Grid resolution is set to have nx points in
                                longitude and ny points in latitude direction.

        dlon, dlat [deg]:       Grid spacing is set as close to the given resolution
                                as possible (edges are fixed).

        dm [m]:                 Grid spacing is set close to dm metres.

        dnmi [nmi]:            Grid spacing is set close to dnmi nautical miles.

        dx, dy [m]:             Grid spacing is set as close as dx and dy metres as
                                possible.

        Set floating_edge=True to force exact dlon, dlat
        and instead possibly move lon_max, lat_max slightly
        to make it work (only compatibel with native coordinates).

        """

        def determine_nx(x_type: str, nx, dx, dm, dlon, dnmi):
            if x_type == "x":
                lon_type = "lon"
            else:
                lon_type = "lat"

            x_end = self.edges(x_type, native=True)[1]

            if nx:
                return int(nx), x_end

            if dnmi:
                if self.core.is_cartesian():
                    dm = dnmi * 1850
                else:
                    dlat = dnmi / 60
                    x_km = distance_funcs.lon_in_km(np.median(self.lat()))
                    y_km = distance_funcs.lat_in_km(np.median(self.lat()))
                    if x_type == "x":
                        dlon = dlat * (y_km / x_km)
                    else:
                        dlon = dlat

            if dlon:
                nx = (
                    np.round((self.edges(lon_type)[1] - self.edges(lon_type)[0]) / dlon)
                    + 1
                )
                if floating_edge:
                    if self.core.is_cartesian():
                        raise Exception(
                            "Grid is cartesian, so cant set exact dlon/dlat using floating_edge!"
                        )
                    x_end = self.edges(lon_type)[0] + (nx - 1) * dlon
                return int(nx), x_end

            if dm:
                dx = dm

            if dx:
                nx = np.round((self.edges(x_type)[1] - self.edges(x_type)[0]) / dx) + 1
                if floating_edge:
                    if not self.core.is_cartesian():
                        raise Exception(
                            "Grid is spherical, so cant set exact dx/dy using floating_edge!"
                        )
                    x_end = self.edges(x_type)[0] + (nx - 1) * dx
                return int(nx), x_end

            # Nothing given
            return len(self.x()), x_end

        nx, native_x_end = determine_nx("x", nx, dx, dm, dlon, dnmi)
        ny, native_y_end = determine_nx("y", ny, dy, dm, dlat, dnmi)

        # Unique to not get [0,0,0] etc. arrays if nx=1
        x_native = np.unique(np.linspace(self.x(native=True)[0], native_x_end, nx))
        y_native = np.unique(np.linspace(self.y(native=True)[0], native_y_end, ny))

        if self.core.is_cartesian():
            x = x_native
            y = y_native
            lon = None
            lat = None
        else:
            lon = x_native
            lat = y_native
            x = None
            y = None

        self._init_structure(x, y, lon, lat, utm=self.utm.zone())

    def _check_mask_right_shape(
        self, mask: np.ndarray, coord: str, **kwargs
    ) -> np.array:
        """Checks that the given mask is same shape as the skeleton.
        Creates a full True maks if mask is None"""
        if mask is None:
            return np.full(self.size("spatial", **kwargs), True)

        mask = np.array(mask)

        if mask.shape != self.size("spatial", **kwargs) and mask.shape != self.shape(
            coord
        ):
            raise ValueError(
                f"Skeleton has shape {self.size('spatial',**kwargs)} and {coord} has shape {self.shape(coord)} but mask is shape {mask.shape}"
            )
        return mask
