import numpy as np
from .skeleton import Skeleton
from .managers.coordinate_manager import CoordinateManager
from .managers.metadata_manager import MetaDataManager
from .managers.dask_manager import DaskManager
from .variables import DataVar, Coordinate
import geo_parameters as gp
import utm as utm_module
from geo_skeletons.aux_funcs import utm_funcs

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
        mask: np.ndarray = None,
    ):

        if mask is None:
            mask = np.full(skeleton.size("spatial"), True)
        dask_manager = DaskManager()
        mask = dask_manager.undask_me(mask)
        lon, lat = skeleton.lonlat(strict=True, mask=mask)
        x, y = skeleton.xy(strict=True, mask=mask)

        new_skeleton = cls(lon=lon, lat=lat, x=x, y=y, name=skeleton.name)
        new_skeleton.utm.set(skeleton.utm.zone(), silent=True)

        return new_skeleton

    def is_gridded(self) -> bool:
        return False

    @staticmethod
    def _initial_coords(spherical: bool = False) -> list[str]:
        """Initial coordinates used with PointSkeletons. Additional coordinates
        can be added by decorators (e.g. @add_time).
        """
        return INITIAL_COORDS

    @staticmethod
    def _initial_vars(spherical: bool = False) -> dict:
        """Initial variables used with PointSkeletons. Additional variables
        can be added by decorator @add_datavar.
        """
        if spherical:
            return INITIAL_SPHERICAL_VARS
        else:
            return INITIAL_CARTESIAN_VARS

    def xgrid(self, native: bool = False, strict: bool = False) -> np.ndarray:
        x, _ = self.xy(native=native, strict=strict)
        return x

    def ygrid(self, native: bool = False, strict: bool = False) -> np.ndarray:
        _, y = self.xy(native=native, strict=strict)
        return y

    def longrid(self, native: bool = False, strict: bool = False) -> np.ndarray:
        lon, _ = self.lonlat(native=native, strict=strict)
        return lon

    def latgrid(self, native: bool = False, strict: bool = False) -> np.ndarray:
        _, lat = self.lonlat(native=native, strict=strict)
        return lat

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
            if normalize:
                x = x - min(x)
            return x

        utm = utm or self.utm.zone()

        lat = self.lat(**kwargs)
        lat = utm_funcs.cap_lat_for_utm(lat)

        posmask = lat >= 0
        negmask = lat < 0
        x = np.zeros(len(lat))
        if np.any(posmask):
            x[posmask], __, __, __ = utm_module.from_latlon(
                lat[posmask],
                self.lon(**kwargs)[posmask],
                force_zone_number=utm[0],
                force_zone_letter=utm[1],
            )
        if np.any(negmask):
            x[negmask], __, __, __ = utm_module.from_latlon(
                -lat[negmask],
                self.lon(**kwargs)[negmask],
                force_zone_number=utm[0],
                force_zone_letter=utm[1],
            )

        if normalize:
            x = x - min(x)

        return x

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

        return self.utm._lon(x=self.x(**kwargs), y=self.y(**kwargs))

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

        return self.utm._lat(x=self.x(**kwargs), y=self.y(**kwargs))

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

        lon, lat = self.lon(native=native, strict=strict, **kwargs), self.lat(
            native=native, strict=strict, **kwargs
        )

        if lon is None:
            return None, None
        if mask is not None:
            return lon[mask], lat[mask]
        return lon, lat

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

        utm = utm or self.utm.zone()

        if self.core.is_cartesian() and (self.utm.zone() == utm):
            y = self._ds_manager.get("y", **kwargs).values.copy()
            if normalize:
                y = y - min(y)
            return y

        posmask = self.lat(**kwargs) >= 0
        negmask = self.lat(**kwargs) < 0

        lat = utm_funcs.cap_lat_for_utm(self.lat(**kwargs))
        y = np.zeros(len(self.lat(**kwargs)))
        if np.any(posmask):
            _, y[posmask], __, __ = utm_module.from_latlon(
                lat[posmask],
                self.lon(**kwargs)[posmask],
                force_zone_number=utm[0],
                force_zone_letter=utm[1],
            )
        if np.any(negmask):
            _, y[negmask], __, __ = utm_module.from_latlon(
                -lat[negmask],
                self.lon(**kwargs)[negmask],
                force_zone_number=utm[0],
                force_zone_letter=utm[1],
            )
            y[negmask] = -y[negmask]

        if normalize:
            y = y - min(y)

        return y

    def xy(
        self,
        mask: np.ndarray = None,
        strict=False,
        normalize: bool = False,
        utm: tuple[int, str] = None,
        **kwargs,
    ) -> tuple[np.ndarray, np.ndarray]:
        """Returns a tuple of x- and y-coordinates of all points.

        If native=True, then lon-lat coordinatites are returned for spherical grids instead
        If strict=True, then (None, None) is returned if grid is spherical

        native=True overrides strict=True for spherical grids

        Identical to (.x(), .y()) (with no mask)
        mask is a boolean array (default True for all points)
        """

        # Transforms x-y to lon-lat if necessary
        x, y = self.x(strict=strict, normalize=normalize, utm=utm, **kwargs), self.y(
            strict=strict, normalize=normalize, utm=utm, **kwargs
        )

        if x is None:
            return None, None

        if mask is not None:
            return x[mask], y[mask]
        return x, y
