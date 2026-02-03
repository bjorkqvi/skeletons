from __future__ import annotations
from typing import TYPE_CHECKING
import numpy as np
from .skeleton import Skeleton
from .managers.coordinate_manager import CoordinateManager
from .managers.metadata_manager import MetaDataManager
from .variables import DataVar, Coordinate
import geo_parameters as gp
from typing import Optional, Union
from .dask_computations import undask_me

from .managers.resample_manager import find_original_skeleton_in_inheritance_chain
from . import distance_funcs
from .errors import MissingDatasetError
inds_coord = Coordinate(name="inds", meta=gp.grid.Inds, coord_group="spatial")
INITIAL_COORDS = [inds_coord]

lon_var = DataVar(name="lon", meta=gp.grid.Lon, coord_group="spatial", default_value=0, grid_mapping='wgs84')
lat_var = DataVar(name="lat", meta=gp.grid.Lat, coord_group="spatial", default_value=0, grid_mapping='wgs84')
x_var = DataVar(name="x", meta=gp.grid.X, coord_group="spatial", default_value=0, grid_mapping='crs')
y_var = DataVar(name="y", meta=gp.grid.Y, coord_group="spatial", default_value=0, grid_mapping='crs')
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

    meta = MetaDataManager(ds_manager=None)
    core = CoordinateManager(INITIAL_COORDS, INITIAL_CARTESIAN_VARS, metadata_manager=meta)
    
    @classmethod
    def from_skeleton(
        cls,
        skeleton: Skeleton,
        proj: Optional[str] = None,
        mask: Optional[np.ndarray] = None,
    ) -> PointSkeleton:
        """Creates a new PointSkeleton containing only points from another Gridded- or PointSkeleton.

        Points can be selected by a boolean mask. No data is transferred"""

        if mask is None:
            mask = np.full(skeleton.size("spatial"), True)
        mask = undask_me(mask)
        
        if proj is None:
            lon, lat = skeleton.lonlat(strict=True, mask=mask)
            x, y = skeleton.xy(strict=True, mask=mask)
        elif proj == 'lonlat':
            lon, lat = skeleton.lonlat(mask=mask)
            x, y = None, None
        elif proj == 'xy':
            lon, lat = None, None
            x, y = skeleton.xy(mask=mask)

        new_skeleton = cls(lon=lon, lat=lat, x=x, y=y, name=skeleton.name)
        if skeleton.proj.crs() is not None:
            new_skeleton.proj.set(skeleton.proj.crs(), silent=True)

        return new_skeleton

    @staticmethod
    def is_gridded() -> bool:
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
        
    def ravel(self, proj: str = None) -> "PointSkeleton":
        cls = find_original_skeleton_in_inheritance_chain(self)
        points = cls.from_skeleton(self, proj=proj)
        return self.resample.grid(points, engine='ravel')
    
    def _quicklook(self, ax, data: np.ndarray, proj: str, contour: bool, cmap: str, vlim: tuple[float]):
        """This is called by the quicklook method of the Skelton class"""
        vmin, vmax = vlim
        if vmin is not None:
            levels = 36
        else:
            levels = (np.ceil(np.max(data)) - np.floor(np.min(data))).astype(int)
        if proj is None:
            x, y = self.xy(native=True)
        elif proj == 'lonlat':
            x, y = self.lonlat()
        elif proj == 'xy':
            x, y = self.xy()

        if contour:
            mask = np.logical_not(np.isnan(data))
            if sum(mask)<3:
                contour = False

        if contour:
            cont = ax.tricontourf(x[mask], y[mask],data[mask], levels=levels, vmin=vmin, vmax=vmax, cmap=cmap)
        else:
            cont = ax.scatter(x, y,c=data, s=2, vmin=vmin, vmax=vmax, cmap=cmap)
        

        return ax, cont


    def _quicklook_quiver(self, ax, arrow_data: np.ndarray, proj:str, var:str, wrt: str, sparse):
        """This is called by the quicklook method of the Skelton class"""
        if proj is None:
            xgrid, ygrid = self.xgrid(native=True), self.ygrid(native=True)
        elif proj == 'lonlat':
            xgrid, ygrid = self.longrid(), self.latgrid()
        elif proj == 'xy':
                xgrid, ygrid = self.xgrid(), self.ygrid()
        if sparse:
            if isinstance(sparse, bool):
                nr_of_points = 500
            else:
                nr_of_points = sparse
            if len(arrow_data.shape) == 1:
                step = np.floor(len(arrow_data)/nr_of_points).astype(int)
                step = np.maximum(step, 1)
                arrow_data = arrow_data[::step]
                xgrid = xgrid[::step]
                ygrid = ygrid[::step]
        ax.quiver(xgrid, ygrid, np.cos(arrow_data), np.sin(arrow_data), label=f"{var}{wrt}")
        return ax

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
        crs: Optional[Union[int, str, dict]] = None,
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
            raise MissingDatasetError

        if not self.core.is_cartesian() and native:
            return self.lon(mask=mask, **kwargs)

        if not self.core.is_cartesian() and strict:
            return None

        if self.core.is_cartesian() and (crs is None or self.proj.crs() == crs):
            x = self._ds_manager.get("x", **kwargs).values.copy()[mask]
        else:
            x = self.proj._x(
                lon=self.lon(mask=mask,crs=self.proj.crs(), **kwargs),
                lat=self.lat(mask=mask,crs=self.proj.crs(), **kwargs),
                crs=crs,
            )


        if normalize:
            x = x - min(x)

        return x

    def y(
        self,
        native: bool = False,
        strict: bool = False,
        mask: Optional[np.ndarray] = None,
        crs: Optional[Union[int, str, dict]] = None,
        normalize: bool = False,
        in_meters: bool = False, 
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
            raise MissingDatasetError

        if not self.core.is_cartesian() and native:
            return self.lat(mask=mask, **kwargs)

        if not self.core.is_cartesian() and strict:
            return None

        if in_meters:
            if crs is not None:
                raise ValueError("Can't both ask ")

        if self.core.is_cartesian() and (crs is None or self.proj.crs() == crs):
            y = self._ds_manager.get("y", **kwargs).values.copy()[mask]
        else:
            y = self.proj._y(
                lon=self.lon(mask=mask, crs=self.proj.crs(),**kwargs),
                lat=self.lat(mask=mask, crs=self.proj.crs(), **kwargs),
                crs=crs,
            )
        


        if normalize:
            y = y - min(y)

        return y

    def lon(
        self,
        native: bool = False,
        strict: bool = False,
        mask: Optional[np.ndarray] = None,
        crs: Optional[Union[int, str, dict]] = None,
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
            raise MissingDatasetError

        if self.core.is_cartesian() and native:
            return self.x(mask=mask, crs=crs, **kwargs)

        if self.core.is_cartesian() and strict:
            return None

        if not self.core.is_cartesian():
            return self._ds_manager.get("lon", **kwargs).values.copy()[mask]

        
        return self.proj._lon(
            x=self.x(mask=mask, **kwargs) ,
            y= self.y(mask=mask, **kwargs) ,
            crs=crs,
        )


    def lat(
        self,
        native: bool = False,
        strict: bool = False,
        mask: Optional[np.ndarray] = None,
        crs: Optional[Union[int, str, dict]] = None,
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
            raise MissingDatasetError

        if self.core.is_cartesian() and native:
            return self.y(mask=mask, crs=crs, **kwargs)

        if self.core.is_cartesian() and strict:
            return None

        if not self.core.is_cartesian():
            return self._ds_manager.get("lat", **kwargs).values.copy()[mask]

        return self.proj._lat(
            x=self.x(mask=mask, **kwargs),
            y=self.y(mask=mask, **kwargs),
            crs=crs,
        )


    def xy(
        self,
        native: bool = False,
        strict: bool = False,
        mask: Optional[np.ndarray] = None,
        crs: Optional[Union[int, str, dict]] = None,
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
            strict=strict, native=native, normalize=normalize, crs=crs, mask=mask, **kwargs
        ), self.y(strict=strict, native=native, normalize=normalize, crs=crs, mask=mask, **kwargs)

        if x is None:
            return None, None

        return x, y

    def lonlat(
        self,
        native: bool = False,
        strict: bool = False,
        mask: Optional[np.ndarray] = None,
        crs: Optional[Union[int, str, dict]] = None,
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
            native=native, strict=strict, mask=mask, crs=crs ,**kwargs
        ), self.lat(native=native, strict=strict, mask=mask,  crs=crs, **kwargs)
        
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

    def resolution(self) -> np.ndarray:
        """Returns an array with the resolution (in metres).
        
        Resolution at a grid point is determined as the distance to nearest neighbour.
        A UTM projection is used to calculate the distances."""
        x, y = self.xy(crs=self.proj.my_utm())
        return distance_funcs.get_dist_point(x, y)
    

    def dy(self, native: bool = False, strict: bool = False, array: bool = True) -> float:
        """Median grid spacing. Conversion made for spherical grids.
        
        Note, methods dx() and dy() are same for cartesian grids"""

        
        if not self.core.is_cartesian() and strict and (not native):
            return None

        if self.ny() == 1:
            return 0.0
        
        if not self.core.is_cartesian() and native:
            return self.dlat()
            
        if not self.proj.units_are_in_degrees() and native:
            return float(np.median(self.resolution()))
        
        return float(np.median(distance_funcs.get_dist_point(self.x(), self.y())))


    
    def dx(self, native: bool = False, strict: bool = False) -> float:
        """Median grid spacing. Conversion made for spherical grids.
        
        Note, methods dx() and dy() are same for cartesian grids"""

        
        if not self.core.is_cartesian() and strict and (not native):
            return None

        if self.nx() == 1:
            return 0.0
        
        if not self.core.is_cartesian() and native:
            return self.dlon()
            
        if not self.proj.units_are_in_degrees() and native:
            return float(np.median(self.resolution()))
        
        return float(np.median(distance_funcs.get_dist_point(self.x(), self.y())))
    

    def dmy(self, native: bool = False, strict: bool = False, array: bool = True) -> float:
        """Median grid spacing. Conversion made for spherical grids.
        
        Note, methods dx() and dy() are same for cartesian grids"""

        
        if (not self.core.is_cartesian() or self.proj.units_are_in_degrees()) and strict and (not native):
            return None
        

        if self.ny() == 1:
            return 0.0
        
        if not self.core.is_cartesian() and native:
            return self.dlat()
            
        if self.proj.units_are_in_degrees() and native:
            return self.dy()
        
        return float(np.median(self.resolution()))
    
    def dmx(self, native: bool = False, strict: bool = False) -> float:
        """Median grid spacing. Conversion made for spherical grids.
        
        Note, methods dx() and dy() are same for cartesian grids"""

        
        if (not self.core.is_cartesian() or self.proj.units_are_in_degrees()) and strict and (not native):
            return None

        if self.nx() == 1:
            return 0.0
        
        if not self.core.is_cartesian() and native:
            return self.dlon()

        if self.proj.units_are_in_degrees() and native:
            return self.dx()

        return float(np.median(self.resolution()))
    
    def dlat(self, native: bool = False, strict: bool = False) -> float:
        """Mean grid spacing of the y vector. Conversion made for spherical grids."""

        
        if self.core.is_cartesian() and strict and (not native):
            return None
        
        if self.core.is_cartesian():
            if native:
                return self.dy()
            if self.proj.crs() is None:
                return None
        
        if self.ny() == 1:
            return 0.0
        
        resolution = self.resolution()
        lon, lat = self.lonlat()
        
        dlats = np.array([distance_funcs.dy_to_dlat(dy=dy, lat=la, lon=lo) for dy, lo, la in zip(resolution, lon, lat)])
        return float(np.median(dlats))

    
    def dlon(self, native: bool = False, strict: bool = False) -> float:
        """Mean grid spacing of the y vector. Conversion made for spherical grids."""

        
        if self.core.is_cartesian() and strict and (not native):
            return None

        if self.core.is_cartesian():
            if native:
                return self.dx()
            if self.proj.crs() is None:
                return None

        if self.nx() == 1:
            return 0.0

        resolution = self.resolution()
        lon, lat = self.lonlat()
        
        dlons = np.array([distance_funcs.dx_to_dlon(dx=dx, lat=la, lon=lo) for dx, lo, la in zip(resolution, lon, lat)])
        return float(np.median(dlons))