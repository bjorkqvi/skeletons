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
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
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


CRSValue = Union[int, str, tuple[int, str], dict]
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
        crs: Optional[CRSValue] = None,
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
        
        crs = crs or skeleton.proj.crs()
        if crs is not None:
            new_skeleton.proj.set(crs, silent=True)

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
        
    def ravel(self, proj: Optional[str] = None, crs: Optional[CRSValue] = None) -> "PointSkeleton":
        cls = find_original_skeleton_in_inheritance_chain(self)
        points = cls.from_skeleton(self, proj=proj, crs=crs)
        return self.resample.grid(points, engine='ravel')
    
    def _quicklook(self, ax, data: np.ndarray, proj: str, contour: bool, cmap: str, vlim: tuple[float]):
        """This is called by the quicklook method of the Skelton class"""
        vmin, vmax = vlim
        if vmin is not None:
            levels = np.arange(0,370,10)
        else:
            levels = np.arange(int(np.floor(np.nanmin(data))), int(np.ceil(np.nanmax(data))+1),1)
            if len(levels) == 0:
                levels = 1
            else:
                
                min_levels = 10
                mul = np.ceil(min_levels/len(levels))
                if mul > 1:
                    spacing = 1/2**(mul-1)
                    levels = np.arange(int(np.floor(np.nanmin(data))), int(np.ceil(np.nanmax(data)))+spacing,spacing)

            if len(levels) == 1:
                levels = np.arange(int(np.round(levels[0]))-1, int(np.round(levels[0]))+2,1)
        if len(levels) < 2 and contour:
            print(f'Need at least two levels to use contour. Setting to False.')
            contour = False
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
            cont = ax.tricontourf(x[mask], y[mask],data[mask], levels=levels, cmap=cmap)
        else:
            if cmap == 'viridis':
                cmap = plt.cm.viridis
            else:
                cmap = plt.cm.twilight
            norm = mcolors.BoundaryNorm(boundaries=levels, ncolors=cmap.N, clip=True)
            cont = ax.scatter(x, y,c=data, s=2,  cmap=cmap, norm=norm)
        

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
    ) -> Union[np.ndarray, None]:
        """Returns a meshgrid of projected x-values.

        This is equivalent to `.x()` since a `PointSkeleton` is not gridded.

        Args:
            native (bool, optional): If `True`, returns longitude values if the Skeleton 
                if spherical. Defaults to `False`.
            strict (bool, optional): If `True`, returns `None` if the Skeleton is spherical. 
                Defaults to `False`.
            mask (Optional[np.ndarray], optional): A boolean mask to filter points. Defaults to `None`.
            normalize (bool, optional): If `True`, normalizes the x-values by subtracting the minimum value. 
                Defaults to `False`.

        Returns:
            np.ndarray: A meshgrid of projected x-values.
        """
        x, _ = self.xy(native=native, strict=strict, normalize=normalize, mask=mask)
        return x

    def ygrid(
        self,
        native: bool = False,
        strict: bool = False,
        mask: Optional[np.ndarray] = None,
        normalize: bool = False,
    ) -> Union[np.ndarray, None]:
        """Returns a meshgrid of projected y-values.

        This is equivalent to `.y()` since a `PointSkeleton` is not gridded.

        Args:
            native (bool, optional): If `True`, returns latitude values if the Skeleton 
                if spherical. Defaults to `False`.
            strict (bool, optional): If `True`, returns `None` if the Skeleton is spherical. 
                Defaults to `False`.
            mask (Optional[np.ndarray], optional): A boolean mask to filter points. Defaults to `None`.
            normalize (bool, optional): If `True`, normalizes the y-values by subtracting the minimum value. 
                Defaults to `False`.

        Returns:
            np.ndarray: A meshgrid of projected y-values.
        """
        _, y = self.xy(native=native, strict=strict, normalize=normalize, mask=mask)
        return y

    def longrid(
        self,
        native: bool = False,
        strict: bool = False,
        mask: Optional[np.ndarray] = None,
    ) -> Union[np.ndarray, None]:
        """Returns a meshgrid of longitude values. For an x-y grids without a projection, returns `None`.

        This is equivalent to `.lon()` since a `PointSkeleton` is not gridded.

        Args:
            native (bool, optional): If `True`, returns projected x-values if the Skeleton is cartesian. 
                Defaults to `False`.
            strict (bool, optional): If `True`, returns `None` if the Skeleton is cartesian. 
                Defaults to `False`.
            mask (Optional[np.ndarray], optional): A boolean mask to filter points. Defaults to `None`.

        Returns:
            np.ndarray: A meshgrid of longitude values.
        """
        lon, _ = self.lonlat(native=native, strict=strict, mask=mask)
        return lon

    def latgrid(
        self,
        native: bool = False,
        strict: bool = False,
        mask: Optional[np.ndarray] = None,
    ) -> Union[np.ndarray, None]:
        """Returns a meshgrid of latitude values. For an x-y grids without a projection, returns `None`.

        This is equivalent to `.lat()` since a `PointSkeleton` is not gridded.

        
        Args:
            native (bool, optional): If `True`, returns projected y-values if the Skeleton is cartesian. 
                Defaults to `False`.
            strict (bool, optional): If `True`, returns `None` if the Skeleton is cartesian. 
                Defaults to `False`.
            mask (Optional[np.ndarray], optional): A boolean mask to filter points. Defaults to `None`.

        Returns:
            np.ndarray: A meshgrid of latitude values.
        """
        _, lat = self.lonlat(native=native, strict=strict, mask=mask)
        return lat

    def x(
        self,
        native: bool = False,
        strict: bool = False,
        crs: Optional[CRSValue] = None,
        mask: Optional[np.ndarray] = None,
        normalize: bool = False,
        **kwargs,
    ) -> Union[np.ndarray, None]:
        """Returns the projected x-coordinate.

        Args:
            native (bool, optional): If `True`, returns longitude values if the Skeleton 
                is spherical. Defaults to `False`.
            strict (bool, optional): If `True`, returns `None` if the Skeleton is spherical. 
                Defaults to `False`.
            crs (Optional[CRSValue]], optional): Specifies the CRS to use for retrieving 
                the x-coordinate. Defaults to the current grid CRS.
            mask (Optional[np.ndarray], optional): A boolean mask to filter points. Defaults to `None`.
            normalize (bool, optional): If `True`, normalizes the x-values by subtracting the minimum value. 
                Defaults to `False`.
            **kwargs: Additional arguments passed to internal methods.

        Returns:
            np.ndarray: The x-coordinate values.
        """

        mask = self._check_mask_right_shape(mask, **kwargs)
        if native and strict:
            raise ValueError("Can't set both 'native' and 'strict' to True!")

        if self.ds() is None:
            raise MissingDatasetError

        if not self.core.is_projected() and native:
            return self.lon(mask=mask, **kwargs)

        if not self.core.is_projected() and strict:
            return None

        if self.core.is_projected() and (crs is None or self.proj.crs() == crs):
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
        crs: Optional[CRSValue] = None,
        normalize: bool = False,
        in_meters: bool = False, 
        **kwargs,
    ) -> Union[np.ndarray, None]:
        """Returns the projected y-coordinate.

        Args:
            native (bool, optional): If `True`, returns latitude values if the Skeleton 
                is spherical. Defaults to `False`.
            strict (bool, optional): If `True`, returns `None` if the Skeleton is spherical. 
                Defaults to `False`.
            crs (Optional[CRSValue], optional): Specifies the CRS to use for retrieving 
                the y-coordinate. Defaults to the current grid CRS.
            mask (Optional[np.ndarray], optional): A boolean mask to filter points. Defaults to `None`.
            normalize (bool, optional): If `True`, normalizes the y-values by subtracting the minimum value. 
                Defaults to `False`.
            in_meters (bool, optional): If `True`, calculates the y-coordinate in meters. Defaults to `False`.
            **kwargs: Additional arguments passed to internal methods.

        Returns:
            np.ndarray: The y-coordinate values.
        """

        mask = self._check_mask_right_shape(mask, **kwargs)

        if native and strict:
            raise ValueError("Can't set both 'native' and 'strict' to True!")
        if self.ds() is None:
            raise MissingDatasetError

        if not self.core.is_projected() and native:
            return self.lat(mask=mask, **kwargs)

        if not self.core.is_projected() and strict:
            return None

        if in_meters:
            if crs is not None:
                raise ValueError("Can't both ask ")

        if self.core.is_projected() and (crs is None or self.proj.crs() == crs):
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
        crs: Optional[CRSValue] = None,
        **kwargs,
    ) -> Union[np.ndarray, None]:
        """Returns the spherical longitude coordinate. For an x-y grid without a projection, returns `None`.

        Args:
            native (bool, optional): If `True`, returns projected x-values if the Skeleton is cartesian. 
                Defaults to `False`.
            strict (bool, optional): If `True`, returns `None` if the Skeleton is cartesian. 
                Defaults to `False`.
            crs (Optional[CRSValue], optional): Specifies the CRS to use for retrieving 
                the longitude. Defaults to the current grid CRS.
            mask (Optional[np.ndarray], optional): A boolean mask to filter points. Defaults to `None`.
            **kwargs: Additional arguments passed to internal methods.

        Returns:
            np.ndarray: The longitude values.
        """
        mask = self._check_mask_right_shape(mask, **kwargs)

        if native and strict:
            raise ValueError("Can't set both 'native' and 'strict' to True!")

        if self.ds() is None:
            raise MissingDatasetError

        if self.core.is_projected() and native:
            return self.x(mask=mask, crs=crs, **kwargs)

        if self.core.is_projected() and strict:
            return None

        if not self.core.is_projected():
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
        crs: Optional[CRSValue] = None,
        **kwargs,
    ) -> Union[np.ndarray, None]:
        """Returns the spherical latitude coordinate. For an x-y grid without a projection, returns `None`.

        Args:
            native (bool, optional): If `True`, returns projected y-values if the Skeleton is cartesian. 
                Defaults to `False`.
            strict (bool, optional): If `True`, returns `None` if the Skeleton is cartesian. 
                Defaults to `False`.
            crs (Optional[CRSValue], optional): Specifies the CRS to use for retrieving 
                the longitude. Defaults to the current grid CRS.
            mask (Optional[np.ndarray], optional): A boolean mask to filter points. Defaults to `None`.
            **kwargs: Additional arguments passed to internal methods.

        Returns:
            np.ndarray: The latitude values.
        """
        mask = self._check_mask_right_shape(mask, **kwargs)

        if native and strict:
            raise ValueError("Can't set both 'native' and 'strict' to True!")

        if self.ds() is None:
            raise MissingDatasetError

        if self.core.is_projected() and native:
            return self.y(mask=mask, crs=crs, **kwargs)

        if self.core.is_projected() and strict:
            return None

        if not self.core.is_projected():
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
        crs: Optional[CRSValue] = None,
        normalize: bool = False,
        **kwargs,
    ) -> tuple[np.ndarray, np.ndarray]:           
        """Returns a tuple of projected x- and y-coordinates for all points.

        Args:
            native (bool, optional): If `True`, returns longitude and latitude values if the Skeleton 
                is spherical. Defaults to `False`.
            strict (bool, optional): If `True`, returns `(None, None)` if the Skeleton is spherical. 
                Defaults to `False`.
            crs (Optional[CRSValue], optional): Specifies the CRS to use for retrieving 
                the coordinates. Defaults to the current grid CRS.
            mask (Optional[np.ndarray], optional): A boolean mask to filter points. Defaults to `None`.
            normalize (bool, optional): If `True`, normalizes the x- and y-values by subtracting 
                the minimum value. Defaults to `False`.
            **kwargs: Additional arguments passed to internal methods.

        Returns:
            tuple[np.ndarray, np.ndarray]: A tuple of x- and y-coordinates.
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
        crs: Optional[CRSValue] = None,
        **kwargs,
        ) -> tuple[np.ndarray, np.ndarray]:
        """Returns a tuple of longitude and latitude for all points.

        Args:
            native (bool, optional): If `True`, returns projected x- and y-values if the Skeleton 
                is cartesian. Defaults to `False`.
            strict (bool, optional): If `True`, returns `(None, None)` if the Skeleton is cartesian. 
                Defaults to `False`.
            crs (Optional[CRSValue], optional): Specifies the CRS to use for retrieving 
                the coordinates. Defaults to the current grid CRS.
            mask (Optional[np.ndarray], optional): A boolean mask to filter points. Defaults to `None`.
            **kwargs: Additional arguments passed to internal methods.

        Returns:
            tuple[np.ndarray, np.ndarray]: A tuple of longitude and latitude values.
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

    def resolution(self, full: bool=False) -> np.ndarray:
        """Returns an array with the resolution (in metres).
        
        Resolution at a grid point is determined as the distance to nearest neighbour.
        A UTM projection is used to calculate the distances.
        
        The resolution is based on a random sample of 10,000 points
        
        set full = True to use all points."""
        x, y = self.xy(crs=self.proj.my_utm())
        if not full and len(x) > 10_000:
            x = np.random.choice(x,10_000, replace=False)
            y = np.random.choice(y,10_000, replace=False)

        return distance_funcs.get_dist_point(x, y)
    

    def dy(self, native: bool = False, strict: bool = False, full: bool = False) -> float:
        """Median grid spacing. Conversion made for spherical grids.
        
        Note, methods dx() and dy() are same for cartesian grids"""

        
        if not self.core.is_projected() and strict and (not native):
            return None

        if self.ny() == 1:
            return 0.0
        
        if not self.core.is_projected() and native:
            return self.dlat()
            
        if self.core.is_cartesian() and native:
            return float(np.median(self.resolution(full=full)))
        
        return float(np.median(distance_funcs.get_dist_point(self.x(), self.y())))


    
    def dx(self, native: bool = False, strict: bool = False, full: bool = False) -> float:
        """Median grid spacing. Conversion made for spherical grids.
        
        Note, methods dx() and dy() are same for cartesian grids"""

        
        if not self.core.is_projected() and strict and (not native):
            return None

        if self.nx() == 1:
            return 0.0
        
        if not self.core.is_projected() and native:
            return self.dlon()
            
        if self.core.is_cartesian() and native:
            return float(np.median(self.resolution(full=full)))
        
        return float(np.median(distance_funcs.get_dist_point(self.x(), self.y())))
    

    def dmy(self, native: bool = False, strict: bool = False, full: bool = False) -> float:
        """Median grid spacing. Conversion made for spherical grids.
        
        Note, methods dx() and dy() are same for cartesian grids"""

        
        if (not self.core.is_cartesian()) and strict and (not native):
            return None
        

        if self.ny() == 1:
            return 0.0
        
        if not self.core.is_projected() and native:
            return self.dlat()
            
        if self.core.is_rotated() and native:
            return self.dy()
        
        return float(np.median(self.resolution(full=full)))
    
    def dmx(self, native: bool = False, strict: bool = False, full: bool = False) -> float:
        """Median grid spacing. Conversion made for spherical grids.
        
        Note, methods dx() and dy() are same for cartesian grids"""

        
        if (not self.core.is_projected() or self.proj.units_are_in_degrees()) and strict and (not native):
            return None

        if self.nx() == 1:
            return 0.0
        
        if not self.core.is_projected() and native:
            return self.dlon()

        if self.core.is_rotated() and native:
            return self.dx()

        return float(np.median(self.resolution(full=full)))
    
    def dlat(self, native: bool = False, strict: bool = False, full: bool = False) -> float:
        """Mean grid spacing of the y vector. Conversion made for spherical grids."""

        
        if self.core.is_projected() and strict and (not native):
            return None
        
        if self.core.is_projected():
            if native:
                return self.dy()
            if self.proj.crs() is None:
                return None
        
        if self.ny() == 1:
            return 0.0
        
        resolution = self.resolution(full=full)
        lon, lat = self.lonlat()
        
        dlats = np.array([distance_funcs.dy_to_dlat(dy=dy, lat=la, lon=lo) for dy, lo, la in zip(resolution, lon, lat)])
        return float(np.median(dlats))

    
    def dlon(self, native: bool = False, strict: bool = False, full: bool = False) -> float:
        """Mean grid spacing of the y vector. Conversion made for spherical grids."""

        
        if self.core.is_projected() and strict and (not native):
            return None

        if self.core.is_projected():
            if native:
                return self.dx()
            if self.proj.crs() is None:
                return None

        if self.nx() == 1:
            return 0.0

        resolution = self.resolution(full=full)
        lon, lat = self.lonlat()
        
        dlons = np.array([distance_funcs.dx_to_dlon(dx=dx, lat=la, lon=lo) for dx, lo, la in zip(resolution, lon, lat)])
        return float(np.median(dlons))