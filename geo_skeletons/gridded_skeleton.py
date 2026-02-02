from __future__ import annotations
from typing import TYPE_CHECKING
import numpy as np
from .skeleton import Skeleton
from .point_skeleton import PointSkeleton
from . import distance_funcs
from .managers.coordinate_manager import CoordinateManager
from .managers.metadata_manager import MetaDataManager
from .variables import Coordinate, DataVar
import geo_parameters as gp
from typing import Optional, Union
from .dask_computations import undask_me
from .errors import SkeletonError, MissingDatasetError
lon_var = Coordinate(name="lon", meta=gp.grid.Lon, coord_group="spatial", grid_mapping='wgs84')
lat_var = Coordinate(name="lat", meta=gp.grid.Lat, coord_group="spatial", grid_mapping='wgs84')
x_var = Coordinate(name="x", meta=gp.grid.X, coord_group="spatial", grid_mapping='crs')
y_var = Coordinate(name="y", meta=gp.grid.Y, coord_group="spatial", grid_mapping='crs')

INITIAL_CARTESIAN_COORDS = [y_var, x_var]
INITIAL_SPERICAL_COORDS = [lat_var, lon_var]

INITIAL_VARS = []
from .distance_funcs import distance_2points

class GriddedSkeleton(Skeleton):
    """Gives a gridded structure to the Skeleton.

    In practise this means that:

    1) Grid coordinates are defined as x,y / lon,lat.
    2) Methods x(), y() / lon(), lat() will return the vectors defining the grid.
    3) Methods xy() / lonlat() will return a list of all points of the grid
    (i.e. raveled meshgrid).
    """

    meta = MetaDataManager(ds_manager=None)
    core = CoordinateManager(INITIAL_CARTESIAN_COORDS, INITIAL_VARS, metadata_manager=meta)

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
        new_skeleton.proj.set(skeleton.proj.crs(), silent=True)

        return new_skeleton

    @staticmethod
    def is_gridded() -> bool:
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

    def ravel(self, proj: str = None) -> "PointSkeleton":
        points = PointSkeleton.from_skeleton(self, proj=proj)
        return self.resample.grid(points, engine='ravel')

    def _quicklook(self, ax, data: np.ndarray, proj: str, contour: bool, arrow_data: np.ndarray):
        """This is called by the quicklook method of the Skelton class"""
        if len(data.shape) == 1:
            print(f'Need true 2D-data (not {data.shape}) to use contour. Setting to False.')
            contour=False
        if proj is None:
            if contour:
                cont = ax.contourf(self.x(native=True), self.y(native=True),data)
            else:
                if len(data.shape) > 1:
                    cont = ax.pcolormesh(self.x(native=True), self.y(native=True),data)
                else:
                    cont = ax.scatter(self.xgrid(native=True), self.ygrid(native=True),c=data, s=2)
            if arrow_data is not None:
                ax.quiver(self.xgrid(native=True), self.ygrid(native=True), np.cos(arrow_data), np.sin(arrow_data))

        elif proj == 'lonlat':
            if contour:
                cont = ax.contourf(self.longrid(), self.latgrid(),data)
            else:
                cont = ax.scatter(self.longrid(), self.latgrid(),c=data, s=2)

            if arrow_data is not None:
                ax.quiver(self.longrid(), self.latgrid(), np.cos(arrow_data), np.sin(arrow_data))


        elif proj == 'xy':
            if contour:
                cont = ax.contourf(self.xgrid(), self.ygrid(),data)
            else:
                s = 15 if arrow_data is not None else 2
                cont = ax.scatter(self.xgrid(), self.ygrid(),c=data, s=s)

            if arrow_data is not None:
                ax.quiver(self.xgrid(), self.ygrid(), np.cos(arrow_data), np.sin(arrow_data))


        return ax, cont

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
        crs: Optional[Union[int, str, dict]] = None,
        **kwargs,
    ) -> np.ndarray:
        """Returns the cartesian x-coordinate.

        If the grid is spherical, a conversion to UTM coordinates is made based on the median latitude.

        strict = True gives 'None' if Skeleton is spherical
        native = True gives longitude values if Skeleton is spherical

        Give 'utm' to get cartesian coordinates in specific UTM-zone. Otherwise defaults to the one set for the grid.
        """
        if native and strict:
            raise ValueError("Can't set both 'native' and 'strict' to True!")
        mask = self._check_mask_right_shape(mask, self.core.x_str, **kwargs)
        vec_mask = np.any(mask, axis=0)

        if self.ds() is None:
            raise MissingDatasetError

        if not self.core.is_cartesian():
            if native:
                return self.lon(**kwargs)
            return None
        
        if (not hasattr(self, 'proj') or self.proj.crs() == crs or crs is None):
            x = self._ds_manager.get("x", **kwargs).values.copy()[vec_mask]
        else:
            return None

        if normalize:
            x = x - min(x)
        return x

    def y(
        self,
        native: bool = False,
        strict: bool = False,
        mask: Optional[np.ndarray] = None,
        normalize: bool = False,
        crs: Optional[Union[int, str, dict]] = None,
        **kwargs,
    ) -> np.ndarray:
        """Returns the cartesian y-coordinate.

        If the grid is spherical, a conversion to UTM coordinates is made based on the median latitude.

        strict = True gives 'None' if Skeleton is spherical
        native = True gives latitude values if Skeleton is spherical

        Give 'utm' to get cartesian coordinates in specific UTM-zone. Otherwise defaults to the one set for the grid.
        """
        if native and strict:
            raise ValueError("Can't set both 'native' and 'strict' to True!")
        mask = self._check_mask_right_shape(mask, self.core.y_str, **kwargs)
        vec_mask = np.any(mask, axis=1)

        if self.ds() is None:
            raise MissingDatasetError

        if not self.core.is_cartesian():
            if native:
                return self.lat(**kwargs)
            return None

        if (not hasattr(self, 'proj') or self.proj.crs() == crs or crs is None):
            y = self._ds_manager.get("y", **kwargs).values.copy()[vec_mask]
        else:
            return None

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

        If the grid is cartesian, a conversion from UTM coordinates is made based on the median y-coordinate.

        strict = True gives 'None' if Skeleton is cartesian
        native = True gives UTM x-values if Skeleton is cartesian
        """
        if native and strict:
            raise ValueError("Can't set both 'native' and 'strict' to True!")
        mask = self._check_mask_right_shape(mask, self.core.x_str, **kwargs)
        vec_mask = np.any(mask, axis=0)

        if self.ds() is None:
            raise MissingDatasetError

        if self.core.is_cartesian():
            if native:
                return self.x(crs=crs, **kwargs)
            return None
        
        return self._ds_manager.get("lon", **kwargs).values.copy()[vec_mask]


    def lat(
        self,
        native: bool = False,
        strict: bool = False,
        mask: Optional[np.ndarray] = None,
        crs: Optional[Union[int, str, dict]] = None,
        **kwargs,
    ) -> np.ndarray:
        """Returns the spherical lat-coordinate. 'None' for cartesian grids that have no UTM-zone.

        If the grid is cartesian, a conversion from UTM coordinates is made based on the median y-coordinate.

        strict = True gives 'None' if Skeleton is cartesian
        native = True gives UTM y-values if Skeleton is cartesian
        """

        if native and strict:
            raise ValueError("Can't set both 'native' and 'strict' to True!")
        mask = self._check_mask_right_shape(mask, self.core.y_str, **kwargs)
        vec_mask = np.any(mask, axis=1)

        if self.ds() is None:
            raise MissingDatasetError

        if self.core.is_cartesian():
            if native:
                return self.y(crs=crs, **kwargs)
            return None
        
        return self._ds_manager.get("lat", **kwargs).values.copy()[vec_mask]


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
        x, y = self._native_xy(**kwargs)

        if self.core.is_cartesian():
            points = PointSkeleton(x=x, y=y)
        else:
            points = PointSkeleton(lon=x, lat=y)
        if self.proj.crs() is not None:    
            points.proj.set(self.proj.crs(), silent=True)
       
        return points.xy(mask=mask, normalize=normalize, native=native, crs = crs or self.proj.crs())

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

        if not self.core.is_cartesian():
            return x[mask], y[mask]
        
        if self.core.is_cartesian() and native:
            return x[mask], y[mask]
            

        points = PointSkeleton(x=x, y=y)
        if self.proj.crs() is None:
            return None, None
        points.proj.set(self.proj.crs(), silent=True)
        return points.lonlat(mask=mask, native=native, crs = crs or self.proj.crs())
    
    def _native_xy(
        self, **kwargs
    ) -> tuple[np.ndarray, np.ndarray]:
        """Returns a tuple of native x and y of all points."""

        x, y = np.meshgrid(
            self.x(native=True, **kwargs),
            self.y(native=True, **kwargs),
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

        def determine_nx(x_type: str, nx: int, dx: float, dlon: float, dnmi: float, floating_edge: bool) -> tuple[int, float]:
            """Determines how many points is needed to get the desired resolution in one dimension
            x_type is 'x' or 'y', determining if we are in x/lon or y/lat direction.
            
            nx: number of points
            dx: cartesian grid spacing in metres (either dx or dy depending on x_type)
            dlon: spherical grid spacing in degrees (either dlon or dlat depending on x_type)
            dnmi: spacing in nautical miles

            floating_edge  [bool]: If True, then spacing is exact and edge of grid is moved.
            
            return nx and right/east (or upper/north) edge of grid"""

            assert x_type in ['x', 'y']
            
            if x_type == "x":
                lon_type = "lon"
            else:
                lon_type = "lat"

            x_end = self.edges(x_type, native=True)[1]

            if nx:
                return int(nx), x_end

            if dnmi:
                if self.core.is_cartesian():
                    dx = dnmi * 1850.0
                else:
                    dlon = dnmi / 60.0
                    if x_type == 'x':
                        lon = sum(self.edges('lon'))/2
                        lat = sum(self.edges('lat'))/2
                        dy = distance_funcs.lat_in_km(lat=lat, lon=lon)*1000*dlon
                        dlon = distance_funcs.dx_to_dlon(dy, lat=lat, lon=lon)
            
            
            # Convert dx/dlon to the native spacing for the grid
            if self.core.is_cartesian():
                if dx: 
                    spacing = dx
                else:
                    if floating_edge:
                        raise SkeletonError(
                            "Grid is cartesian, so cant set exact dlon/dlat using floating_edge!"
                        )
                    
                    # Reproject edges to get mid point of grid in spherical coordinates
                    x = self.edges('x')
                    y = self.edges('y')
                    points = PointSkeleton(x=x, y=y, crs=self.proj.crs())
                    lon, lat = points.lonlat()
                    if lon is None:
                        raise SkeletonError("Can't set spacing with dlon/dlat since there is not projection information for the grid!")
                    lon, lat = sum(lon)/2, sum(lat)/2
                    if x_type == 'x':
                        spacing = distance_funcs.dlon_to_dx(dlon, lat=lat, lon=lon)
                    else:
                        spacing = distance_funcs.dlat_to_dy(dlon, lat=lat, lon=lon)
            elif not self.core.is_cartesian():
                if dlon:
                    spacing = dlon
                else:
                    if floating_edge:
                        raise SkeletonError(
                            "Grid is spherical, so cant set exact dx/dy using floating_edge!"
                        )
                    
                    lon = self.edges('lon')
                    lat = self.edges('lat')
                    lon, lat = sum(lon)/2, sum(lat)/2
                    if x_type == 'x':
                        spacing = distance_funcs.dx_to_dlon(dx, lat=lat, lon=lon)
                    else:
                        spacing = distance_funcs.dy_to_dlat(dx, lat=lat, lon=lon)

             
            nx = (
                np.round((self.edges(lon_type, native=True)[1] - self.edges(lon_type, native=True)[0]) / spacing)
                + 1
            )
            if floating_edge:
                x_end = self.edges(x_type, native=True)[0] + (nx - 1) * spacing

            return nx.astype(int), x_end
        
        if dm:
            dx, dy = dm, dm

        if any([nx, dx, dlon, dnmi]):
            nx, native_x_end = determine_nx("x", nx, dx, dlon, dnmi, floating_edge)
        else:
            nx, native_x_end = len(self.x(native=True)), self.edges('x', native=True)[-1]
        if any([ny, dy, dlat, dnmi]):
            ny, native_y_end = determine_nx("y", ny, dy, dlat, dnmi, floating_edge)
        else:
            ny, native_y_end = len(self.y(native=True)), self.edges('y', native=True)[-1]

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

        old_metadata = self.meta._metadata
        self._init_structure(x, y, lon, lat)
        self.meta.set_by_dict(old_metadata)

    def dmx(self,native: bool = False, strict: bool = False) -> float:
        """The grid distance in metres for the x-direction.
        
        For cartesian grids this method is identical to .dx()
        For grids with non cartesian projectsion (e.g. rotated pole), .dx() will give """

        if (not self.core.is_cartesian() or self.proj.units_are_in_degrees()) and strict and (not native):
            return None
        if self.nx() == 1:
            return 0.0

        if self.core.is_cartesian() and not self.proj.units_are_in_degrees():
            x = self.edges('x')
            
            return float((x[1]-x[0])/(self.nx()-1))
        else:
            if native:
                if self.core.is_cartesian():
                    return self.dx()
                else:
                    return self.dlon()
            midpoint = np.floor(self.ny()/2).astype(int)
            data_slice = self.isel(**{self.core.y_str:midpoint})
            
            lat = data_slice.edges('lat')
            lon = data_slice.edges('lon')

            d = distance_2points(lat[0], lon[0], lat[1], lon[1]) 
            return float(d/(data_slice.nx()-1))

    def dmy(self,native: bool = False, strict: bool = False) -> float:
        """The grid distance in metres for the y-direction.
        
        For cartesian grids this method is identical to .dy()
        For grids with non cartesian projectsion (e.g. rotated pole), .dy() will give """

        if (not self.core.is_cartesian() or self.proj.units_are_in_degrees()) and strict and (not native):
            return None
        
        if self.ny() == 1:
            return 0.0

        if self.core.is_cartesian() and not self.proj.units_are_in_degrees():
            y = self.edges('y')
            return float((y[1]-y[0])/(self.ny()-1))

        if native:
            if self.core.is_cartesian():
                return self.dy()
            else:
                return self.dlat()
            
        midpoint = np.floor(self.nx()/2).astype(int)
        data_slice = self.isel(**{self.core.x_str:midpoint})
        
        lat = data_slice.edges('lat')
        lon = data_slice.edges('lon')
        d = distance_2points(lat[0], lon[0], lat[1], lon[1]) 
        return float(d/(data_slice.ny()-1))


    def dy(self, native: bool = False, strict: bool = False) -> float:
        """Mean grid spacing of the y vector. Conversion made for spherical grids."""
        
        if not self.core.is_cartesian() and strict and (not native):
            return None

        if self.ny() == 1:
            return 0.0

        
        if self.core.is_cartesian():
            y = self.edges('y')
            return float((y[1]-y[0])/(self.ny()-1))

        if native:
            return self.dlat()
        
        if self.proj.units_are_in_degrees():
            # Spherical grid with rotated pole (units in degrees, not meter)
            midpoint = np.floor(self.nx()/2).astype(int)
            data_slice = self.isel(lon=midpoint)
            
            rlat = data_slice.edges('y')
            return float((rlat[1]-rlat[0])/(self.ny()-1))

        # Spherical grid with cartesian (e.g. UTM) projection
        midpoint = np.floor(self.nx()/2).astype(int)
        data_slice = self.isel(lon=midpoint)
        lon = data_slice.lon()[0]
        lat = data_slice.edges('lat')
        d = distance_2points(lat[0], lon, lat[1], lon) 
        return float(d/(self.ny()-1))


    def dx(self, native: bool = False, strict: bool = False) -> float:
        """Mean grid spacing of the x vector. Conversion made for spherical grids."""
        if not self.core.is_cartesian() and strict and (not native):
            return None

        if self.nx() == 1:
            return 0.0

        
        if self.core.is_cartesian():
            x = self.edges('x')
            return float((x[1]-x[0])/(self.nx()-1))

        if native:
            return self.dlon()
            
        if self.proj.units_are_in_degrees():
            # Spherical grid with rotated pole (units in degrees, not meter)
            midpoint = np.floor(self.ny()/2).astype(int)
            data_slice = self.isel(lat=midpoint)
            rlon = data_slice.edges('x')
            return float((rlon[1]-rlon[0])/(self.nx()-1))

        # Spherical grid with cartesian (e.g. UTM) projection
        midpoint = np.floor(self.ny()/2).astype(int)
        data_slice = self.isel(lat=midpoint)
        lon = data_slice.edges('lon')
        lat = data_slice.lat()[0]
        d = distance_2points(lat, lon[0], lat, lon[1]) 
        return float(d/(self.nx()-1))

    def dlat(self, native: bool = False, strict: bool = False):
        """Mean grid spacing of the latitude vector. Conversion made for
        cartesian grids."""
        if self.core.is_cartesian() and strict and (not native):
            return None
        
        if self.ny() == 1:
            return 0.0
        
        if not self.core.is_cartesian():
            lat = self.edges('lat')
            return float((lat[1]-lat[0])/(self.ny()-1))

        if native:
            return self.dy()
        midpoint = np.floor(self.nx()/2).astype(int)
        data_slice = self.isel(x=midpoint)
        lon, lat = data_slice.lonlat()

        if lat is None: # Cartesian grid with no set projection
            return None
        
        lon, lat = np.median(lon), np.median(lat)
        
        if self.proj.units_are_in_degrees():
            # Non-spherical grid with rotated pole (units in degrees, not meter)
            dmy = data_slice.dmy()
        else:
            dmy = self.dmy()

        return distance_funcs.dy_to_dlat(dy=dmy, lat=lat, lon=lon)

    def dlon(self, native: bool = False, strict: bool = False):
        """Mean grid spacing of the latitude vector. Conversion made for
        cartesian grids."""
        if self.core.is_cartesian() and strict and (not native):
            return None
        
        if self.nx() == 1:
            return 0.0
        
        if not self.core.is_cartesian():
            lon = self.edges('lon')
            return float((lon[1]-lon[0])/(self.nx()-1))

        if native:
            return self.dx()
        

        midpoint = np.floor(self.ny()/2).astype(int)
        data_slice = self.isel(y=midpoint)
        lon, lat = data_slice.lonlat()
        
        if lon is None: # Cartesian grid with no set projection
            return None
        
        lon, lat = np.median(lon), np.median(lat)
        
        if self.proj.units_are_in_degrees():
            # Non-spherical grid with rotated pole (units in degrees, not meter)
            dmx = data_slice.dmx()
        else:
            dmx = self.dmx()

        return distance_funcs.dx_to_dlon(dx=dmx, lat=lat, lon=lon)


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
