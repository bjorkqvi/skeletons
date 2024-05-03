import numpy as np
from .skeleton import Skeleton
from .point_skeleton import PointSkeleton
from .aux_funcs import distance_funcs
from .managers.coordinate_manager import CoordinateManager
from .managers.dask_manager import DaskManager
from .managers.metadata_manager import MetaDataManager
from .variables import Coordinate
import geo_parameters as gp
import utm as utm_module
from geo_skeletons.aux_funcs import utm_funcs

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
        mask: np.ndarray = None,
    ):
        if not skeleton.is_gridded():
            raise Exception(
                "Can't create a GriddedSkeleton from a non-gridded data structure!"
            )

        if mask is None:
            mask = np.full(skeleton.size("spatial"), True)
        dask_manager = DaskManager()
        mask = dask_manager.undask_me(mask)

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
    def _initial_coords(spherical: bool = False) -> list[str]:
        """Initial coordinates used with GriddedSkeletons. Additional coordinates
        can be added by decorators (e.g. @add_time).
        """
        if spherical:
            return INITIAL_SPERICAL_COORDS
        else:
            return INITIAL_CARTESIAN_COORDS

    @staticmethod
    def _initial_vars(spherical: bool = False) -> dict:
        """Initial coordinates used with GriddedSkeletons. Additional variables
        can be added by decorator @add_datavar.
        """
        return INITIAL_VARS

    def xgrid(self, native: bool = False, strict: bool = False) -> np.ndarray:
        """Returns a meshgrid of x-values"""
        if self.x(strict=strict) is None:
            return None
        X, _ = np.meshgrid(self.x(native=native), self.y(native=native))
        return X

    def ygrid(self, native: bool = False, strict: bool = False) -> np.ndarray:
        """Returns a meshgrid of y-values"""
        if self.y(strict=strict) is None:
            return None
        _, Y = np.meshgrid(self.x(native=native), self.y(native=native))
        return Y

    def longrid(self, native: bool = False, strict: bool = False) -> np.ndarray:
        """Returns a meshgrid of x-values"""
        if self.lon(strict=strict) is None:
            return None
        LON, _ = np.meshgrid(self.lon(native=native), self.lat(native=native))
        return LON

    def latgrid(self, native: bool = False, strict: bool = False) -> np.ndarray:
        """Returns a meshgrid of y-values"""
        if self.lat(strict=strict) is None:
            return None
        _, LAT = np.meshgrid(self.lon(native=native), self.lat(native=native))
        return LAT

    def lon(self, native: bool = False, strict=False, **kwargs) -> np.ndarray:
        """Returns the spherical lon-coordinate.

        If the grid is cartesian, a conversion from UTM coordinates is made based on the medain y-coordinate.

        If native=True, then x-coordinatites are returned for cartesian grids instead
        If strict=True, then None is returned if grid is cartesian

        native=True overrides strict=True for cartesian grids
        """
        if self.ds() is None:
            return None

        if self.core.is_cartesian() and native:
            return self.x(**kwargs)

        if self.core.is_cartesian() and strict:
            return None

        if not self.core.is_cartesian():
            return self._ds_manager.get("lon", **kwargs).values.copy()

        print(
            "Regridding cartesian grid to spherical coordinates will cause a rotation! Use 'lon, _ = skeleton.lonlat()' to get a list of all points."
        )
        return self.utm._lon(x=self.x(**kwargs), y=np.median(self.y(**kwargs)))

    def lat(self, native: bool = False, strict=False, **kwargs) -> np.ndarray:
        """Returns the spherical lat-coordinate.

        If the grid is cartesian, a conversion from UTM coordinates is made based on the medain y-coordinate.

        If native=True, then y-coordinatites are returned for cartesian grids instead
        If strict=True, then None is returned if grid is cartesian

        native=True overrides strict=True for cartesian grids
        """
        if self.ds() is None:
            return None

        if self.core.is_cartesian() and native:
            return self.y(**kwargs)

        if self.core.is_cartesian() and strict:
            return None

        if not self.core.is_cartesian():
            return self._ds_manager.get("lat", **kwargs).values.copy()
        print(
            "Regridding cartesian grid to spherical coordinates will cause a rotation! Use '_, lat = skeleton.lonlat()' to get a list of all points."
        )

        return self.utm._lat(x=np.median(self.x(**kwargs)), y=self.y(**kwargs))

    def lonlat(
        self,
        mask: np.ndarray = None,
        order_by: str = "lat",
        native: bool = False,
        strict: bool = False,
        **kwargs,
    ) -> tuple[np.ndarray, np.ndarray]:
        """Returns a tuple of lon and lat of all points.
        If strict=True, then None is returned if grid is sperical.

        mask is a boolean array (default True for all points)
        order_by = 'y' (default) or 'x'
        """

        if self.core.is_cartesian() and strict and (not native):
            return None, None

        if mask is None:

            mask = np.full(super().size("spatial", **kwargs), True)

        mask = mask.ravel()
        x, y = self._native_xy(**kwargs)

        if not self.core.is_cartesian() or native:
            return x[mask], y[mask]

        # Only convert if skeleton is Cartesian and native output is not requested
        points = PointSkeleton(x=x, y=y)
        points.utm.set(self.utm.zone(), silent=True)

        return points.lonlat(mask=mask)

    def x(
        self,
        native: bool = False,
        strict: bool = False,
        normalize: bool = False,
        utm: tuple[int, str] = None,
        **kwargs,
    ) -> np.ndarray:
        """Returns the cartesian x-coordinate.

        If the grid is spherical, a conversion to UTM coordinates is made based on the medain latitude.

        If native=True, then longitudes are returned for spherical grids instead
        If strict=True, then None is returned if grid is sperical

        native=True overrides strict=True for spherical grids

        Give utm to get cartesian coordinates in specific utm system. Otherwise defaults to the one set for the grid.
        """

        if self.ds() is None:
            return None

        if not self.core.is_cartesian() and native:
            return self.lon(**kwargs)

        if not self.core.is_cartesian() and strict:
            return None

        if self.core.is_cartesian() and (self.utm.zone() == utm or utm is None):
            x = self._ds_manager.get("x", **kwargs).values.copy()
        else:
            x = self.utm._x(
                lon=self.lon(**kwargs), lat=np.median(self.lat(**kwargs)), utm=utm
            )

        if normalize:
            x = x - min(x)
        return x

    def y(
        self,
        native: bool = False,
        strict: bool = False,
        normalize: bool = False,
        utm: tuple[int, str] = None,
        **kwargs,
    ) -> np.ndarray:
        """Returns the cartesian y-coordinate.

        If the grid is spherical, a conversion to UTM coordinates is made based on the medain latitude.

        If native=True, then latitudes are returned for spherical grids instead
        If strict=True, then None is returned if grid is sperical

        native=True overrides strict=True for spherical grids

        Give utm to get cartesian coordinates in specific utm system. Otherwise defaults to the one set for the grid.
        """

        if self.ds() is None:
            return None

        if not self.core.is_cartesian() and native:
            return self.lat(**kwargs)

        if not self.core.is_cartesian() and strict:
            return None

        if self.core.is_cartesian() and (self.utm.zone() == utm or utm is None):
            y = self._ds_manager.get("y", **kwargs).values.copy()
        else:
            y = self.utm._y(
                lon=np.median(self.lon(**kwargs)), lat=self.lat(**kwargs), utm=utm
            )

        if normalize:
            y = y - min(y)

        return y

    def xy(
        self,
        mask: np.ndarray = None,
        order_by: str = "y",
        native: bool = False,
        strict: bool = False,
        normalize: bool = False,
        utm: tuple[int, str] = None,
        **kwargs,
    ) -> tuple[np.ndarray, np.ndarray]:
        """Returns a tuple of x and y of all points.
        If strict=True, then None is returned if grid is sperical.

        mask is a boolean array (default True for all points)
        order_by = 'y' (default) or 'x'
        """

        if not self.core.is_cartesian() and strict and (not native):
            return None, None

        if mask is None:
            mask = np.full(super().size("spatial", **kwargs), True)

        mask = mask.ravel()

        x, y = self._native_xy(utm=utm, **kwargs)
        if self.core.is_cartesian() or native:
            return x[mask], y[mask]

        # Only convert if skeleton is not Cartesian and native output is not requested
        points = PointSkeleton(lon=x, lat=y)
        points.utm.set(self.utm.zone(), silent=True)

        return points.xy(mask=mask)

    def _native_xy(
        self, utm: tuple[int, str] = None, **kwargs
    ) -> tuple[np.ndarray, np.ndarray]:
        """Returns a tuple of native x and y of all points."""

        x, y = np.meshgrid(
            self.x(native=True, utm=utm, **kwargs),
            self.y(native=True, utm=utm, **kwargs),
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


# dummy(time, y, x)
