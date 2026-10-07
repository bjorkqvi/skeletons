from .coordinate_manager import CoordinateManager
from typing import Any
from .metadata_manager import MetaDataManager
from typing import Optional, Union
from pyproj import CRS, Transformer
from pyproj.exceptions import CRSError
import numpy as np
import utm as utm_module
import xarray as xr
from geo_skeletons.errors import ProjectionError
from geo_parameters.metaparameter import MetaParameter
from .. import logger
VALID_UTM_ZONES = [
    "C",
    "D",
    "E",
    "F",
    "G",
    "H",
    "J",
    "K",
    "L",
    "M",
    "N",
    "P",
    "Q",
    "R",
    "S",
    "T",
    "U",
    "V",
    "W",
    "X",
]
from functools import lru_cache

SOUTH_UTM_ZONES = ["C", "D", "E", "F", "G", "H", "J", "K", "L", "M"]

VALID_UTM_NUMBERS = np.linspace(1, 60, 60).astype(int)
from functools import lru_cache
CRSValue = Union[int, str, tuple[int, str], dict, CRS]

def decode_crs(crs:Optional[Union[str, int]]=None) -> tuple[int, str, dict]:
    """Decodes the give crs hat can be either EPSG code (int) or roj4 string (str) to an EPSG code and proj4 string (and None)"""
    if crs is None:
        return None, None, None, None, None
    
    if isinstance(crs, CRS):
        return None, None, None, None, crs
    
    if isinstance(crs, dict):
        if set(crs.keys()) == {'proj4'}:
            return None, crs.get('proj4'), None, None, None
        return None, None, crs, None, None
    
    if isinstance(crs, int):
        return crs, None, None, None, None

    # UTM zone
    if isinstance(crs, tuple):
        return None, None, None, crs, None

    if not isinstance(crs, str):
        raise ValueError(f"crs needs to be of type 'int' or 'str' or tuple[int, str], not {crs}!")

    if crs[0:4] == 'EPSG':
        return int(crs[5:]), None, None, None, None
    

    
    return None, crs, None, None, None

def _determine_crs(crs: Optional[CRSValue], utm: Optional[tuple[int, str]], is_projected: bool) -> CRSValue:
    """Determines the appropriate CRS based on the given inputs."""
    if not is_projected:
        if crs is not None and crs != CRS.from_epsg(4326):
            raise ProjectionError("Cannot set a new CRS for a skeleton defined with lon-lat!")
            
        return CRS.from_epsg(4326)
    if crs is not None:
        return crs
    if utm is not None:
        return utm
    wkt = (
        'ENGINEERINGCRS["Local Cartesian", '
        'ENGINEERINGDATUM["Local datum"], '
        'CS[Cartesian,2], '
        'AXIS["x",east], AXIS["y",north], '
        'LENGTHUNIT["metre",1]]'
    )
    return CRS.from_wkt(wkt)

class ProjManager:
    def __init__(
        self,  lat: np.ndarray[np.float64], lon: np.ndarray[np.float64],y: np.ndarray[np.float64], x: np.ndarray[np.float64], metadata_manager: MetaDataManager, coordinate_manager: CoordinateManager, crs: Optional[Union[int, str]]=None, utm: Optional[tuple[int, str]]=None
    ):
        if lat is None:
            lat = (None, None)
        if lon is None:
            lon = (None, None)
        if x is None:
            x = (None, None)
        if y is None:
            y = (None, None)
        
        self._lat_values: np.ndarray[np.float64] = lat
        self._lon_values: np.ndarray[np.float64] = lon      
        self._y_values: np.ndarray[np.float64] = y
        self._x_values: np.ndarray[np.float64] = x

        self._meta: MetaDataManager = metadata_manager
        self._coordinate_manager: CoordinateManager = coordinate_manager
        self._crs = None
        self._utm = None

        crs = _determine_crs(crs, utm, self._coordinate_manager.is_projected())

        self.set(crs)
        if not isinstance(self.crs(), tuple):
            self.reset_utm()
        self._alpha: dict = {}
        self._original_ds_proj_data = None

    def _is_valid_utm(self, utm: tuple[int, str]) -> bool:
        """Checks that the given utm zone is valid"""
        if len(utm) != 2:
            return False
        if not utm[0] in VALID_UTM_NUMBERS:
            return False
        if not utm[1] in VALID_UTM_ZONES:
            return False
        return True

    def _optimal_utm(self, lon: np.ndarray, lat: np.ndarray) -> tuple[int, str]:
        """Determines an optimat UTM-zone given longitude and latitude coordinates."""
        lat = np.array(lat)
        lon = np.array(lon)

        mask = np.logical_and(lat <= 84, lat >= -80)
        if np.all(np.logical_not(mask)):
            return None

        lat = lat[mask]
        lon = lon[mask]

        try:
            __, __, zone_number, zone_letter = utm_module.from_latlon(lat, lon)
        except ValueError:  # ValueError: latitudes must all have the same sign
            __, __, zone_number, zone_letter = utm_module.from_latlon(
                np.median(lat), np.median(lon)
            )
        return (zone_number, zone_letter)

    def optimal_utm(self) -> tuple[int, str]:
        """Returns the optimal UTM (Universal Transverse Mercator) zone based on the grid's longitude and latitude edges."""
        if not isinstance(self.crs(), tuple) and self.crs().is_engineering:
            return None
 
        if len(self._lon_values) == 2 and tuple(self._lon_values) == (None, None):
            lon, lat = self._lonlat(tuple(self._x_values), tuple(self._y_values), self.crs())
        else:
            lon, lat = self._lon_values, self._lat_values
        return self._optimal_utm(lon, lat)

    def utm(self) -> Union[tuple[int, str], None]:
        """Returns the UTM (Universal Transverse Mercator) zone for the grid.

        1) If the CRS is not UTM it calculates and returns the optimal UTM zone based on the grid's 
        longitude and latitude edges. 
        2) If no UTM zone is set and the grid is undefined, it returns `None`.

        Returns:
            Optional[tuple]: The UTM zone as a tuple (e.g., `(33, 'N')`) if determined, 
            or `None` if no UTM zone can be calculated.
        """
        if self._utm is not None:
            return self._utm

        return self.optimal_utm()

    def reset_utm(self, verbose: Optional[bool] = None) -> None:
        """Resets the UTM zone based on the grid's longitude and latitude edges.

        This method recalculates the UTM zone using the current longitude and latitude 
        edges of the grid. The latitude edges are capped to above -80° and below 84°).

        Args:
            verbose (bool, optional): If `True`, provides detailed output during the process. If `None`, falls back to the global verbosity setting. Defaults to the global setting, which is `False` unless explicitly changed.

        Returns:
            None: This method modifies the object's UTM zone in place.
        """

        self._utm = self.optimal_utm()
        if logger.is_verbose(verbose) and self._utm is not None:
            logger.log.info(f"Setting UTM {self._utm}")

    def units_are_in_degrees(self) -> bool:
        """Determines whether the current CRS projection is in degrees.

        This method checks if the current coordinate reference system (CRS) is geographic 
        (i.e., the units are in degrees). 
        
        For Cartesian projections (e.g., UTM), it returns `False`.

        Notes:
            - Both regular lon-lat and e.g. rotated poles are in degrees and will return `True`
            - If no projection is set in an x-y grid it is assumed to be cartesian and will return `False`

        Returns:
            bool: `True` if the CRS is geographic (units in degrees), `False` otherwise.
        """
        if isinstance(self.crs(), tuple):
            return False
        
        return self.crs().is_geographic

    def set_utm(self, utm:tuple[int, str], verbose: Optional[bool] = None) -> None:
        """Sets new UTM zone"""    
        if not isinstance(utm, tuple) or len(utm) != 2 or not isinstance(utm[0], int) or not isinstance(utm[1], str):
            raise ValueError(f"utm needs to be a tuple[int, str], not {utm}!")

        if logger.is_verbose(verbose):
                logger.log.info(f"Setting UTM {utm}")
        
        self._utm = utm

        if isinstance(self.crs(), tuple) or self.is_engineering():
            # Set CRS to new UTM to not have conflicting values
            self.set(utm, verbose=verbose)

    def set(self, crs: CRSValue, verbose: Optional[bool] = None) -> None:
        """Sets the Coordinate Reference System (CRS) for the object.

        This method sets the CRS based on various input formats, such as an EPSG code, 
        a proj4 string, a CF-compliant dictionary, a UTM zone, or a CRS object. The method 
        also updates the object's metadata and adjusts units for `x` and `y` variables if 
        the CRS is geographic (i.e., units are in degrees).

        Args:
            crs (CRSValue): The CRS to set. Accepted formats include:
                - EPSG code (e.g., `4326` or `"EPSG:4326"`).
                - Proj4 string (e.g., `"+proj=longlat +datum=WGS84 +no_defs"`).
                - CF-compliant dictionary (e.g., `{'grid_mapping_name': 'latitude_longitude'}`).
                - UTM zone as a tuple (e.g., `(33, 'N')`).
                - CRS object (e.g., `pyproj.CRS` instance).
            verbose (bool, optional): If `True`, outputs print statements that indicate 
                the CRS being set. Defaults to global state, which is by default `False`.

        Returns:
            None: This method modifies the object's CRS in place.

        Raises:
            ValueError: If the provided CRS is not valid or cannot be decoded.

        Notes:
            - If the CRS is set using an EPSG code, proj4 string, or CF-compliant dictionary, 
            the metadata is updated to reflect the CRS.
            - For UTM zones, the metadata is updated with the `utm_zone` and `utm_letter`.
            - If the CRS is geographic (units in degrees), the units of the `x` and `y` 
            variables are updated to `'degrees'`.

        """
        if self.crs() == self.to_crs(4326):
            raise ProjectionError("Cannot set a new CRS for a skeleton defined with lon-lat!")

        epsg, proj4, cf_dict, utm, crs_obj = decode_crs(crs)
        
        if epsg is not None:
            if logger.is_verbose(verbose):
                logger.log.info(f"CRS: EPSG code {epsg}")
            if self._meta is not None:
                self._meta.set({'epsg':epsg},'crs')
        elif proj4 is not None:
            if logger.is_verbose(verbose):
                logger.log.info(f"CRS: proj4 string {proj4}")
            if self._meta is not None:
                self._meta.set({'proj4':proj4},'crs')
        elif cf_dict is not None:
            if logger.is_verbose(verbose):
                logger.log.info(f"CRS: projection from dict {cf_dict}")
            if self._meta is not None:
                self._meta.set(cf_dict,'crs')
        elif utm is not None:
            if logger.is_verbose(verbose):
                logger.log.info(f"CRS: UTM {utm}")
            if self._meta is not None:
                self._meta.set({"utm_zone": utm[0], 'utm_letter': utm[1]},'crs')
        elif crs_obj is not None:
            if logger.is_verbose(verbose):
                logger.log.info(f"CRS: {crs_obj}")
            if self._meta is not None:
                try:
                    self._meta.set(crs.to_cf(),'crs')
                except KeyError: # *** KeyError: 'o_lon_p'
                    self._meta.set(crs.to_dict(),'crs')
        else:
            raise ValueError(f"{crs} is not a valid coordinate reference system!")
        self._crs = crs
        self._meta.set(self.crs_attrs(crs),'crs')
        if utm is not None and self._utm != utm:
            self.set_utm(utm, verbose=verbose)
        
        # Set correct units for variables x and y if we are in a rotated grid
        if self._coordinate_manager.is_projected():
            if self.units_are_in_degrees():
                self._coordinate_manager._switch_to_rotated()
            else:
                self._coordinate_manager._switch_to_not_rotated()
                
        
            self._meta.set(self._coordinate_manager.get('x').meta.meta_dict(),'x')
            self._meta.set(self._coordinate_manager.get('y').meta.meta_dict(),'y')
    def is_engineering(self) -> bool:
        """Checks if the current CRS is an engineering CRS."""
        crs = self.crs()
        return not isinstance(crs, tuple) and crs.is_engineering

    def crs_attrs(self, crs: Optional[CRSValue]=None) -> dict:
        """Returns the CRS attributes as a dictionary suitable for NetCDF."""
        if crs is None:
            crs = self.crs()
        else:
            crs = self.to_crs(crs)
        return crs_to_netcdf_attrs(crs)


    def to_crs(self, crs: Optional[Union[str, int, dict]]=None) -> Union[CRS, tuple[int, str], None]:
        """Converts the given CRS into a pyproj CRS object or UTM tuple.

        This method takes a CRS in various formats (e.g., EPSG code, proj4 string, or CF-compliant dictionary) 
        and returns it as a pyproj CRS object or UTM zone tuple. If the input CRS is invalid or not provided, 
        `None` is returned.

        Args:
            crs (Optional[Union[str, int, dict]], optional): The CRS to convert. Accepted formats include:
                - EPSG code (e.g., `4326` or `"EPSG:4326"`).
                - Proj4 string (e.g., `"+proj=longlat +datum=WGS84 +no_defs"`).
                - CF-compliant dictionary (e.g., `{'grid_mapping_name': 'latitude_longitude'}`).
                - If `None`, returns `None`. Defaults to `None`.

        Returns:
            Union[CRS, tuple[int, str], None]: 
                - A pyproj CRS object representing the CRS if successfully converted.
                - A tuple representing the UTM zone (e.g., `(33, 'N')`) if applicable.
                - `None` if the CRS cannot be determined or is invalid.

        Notes:
            - A UTM tuple is returned if the CRS represents a UTM zone.
            - If the provided CRS is already a valid pyproj CRS object, it is returned as-is.
        """
        epsg, proj4, cf_dict, utm, crs = decode_crs(crs)

        if crs is not None:
            return crs
        elif epsg is not None:
            return CRS.from_epsg(epsg)
        elif proj4 is not None:
            return CRS.from_proj4(proj4)
        elif cf_dict is not None:
            return CRS.from_cf(cf_dict) 
        elif utm is not None:
            return utm
        else:
            return None
        
    def crs(self) -> Union[CRS, tuple[int, str], None]:
        """Returns the current CRS as a pyproj CRS object or UTM tuple.

        This method retrieves the currently set Coordinate Reference System (CRS) for the object, 
        returning it as a pyproj CRS object or UTM tuple. If no CRS is set, it returns `None`.

        Returns:
            Union[CRS, tuple[int, str], None]: 
                - A pyproj CRS object representing the current CRS.
                - A tuple representing the UTM zone (e.g., `(33, 'N')`) if applicable.
                - `None` if no CRS is set.

        Notes:
            - The UTM zone is returned as a tuple if the current CRS represents a UTM projection.
        """
        return self.to_crs(self._crs)


    @lru_cache(maxsize=10)
    def _lonlat(self, x: tuple[float], y: tuple[float], crs: Union[int, str]) -> tuple[np.ndarray, np.ndarray]:
        """Calculates lon and lat coordinates based on given projected x,y-coordinates and the set (or given) CRS projection"""
        if not isinstance(crs, tuple):
            return self._pyproj_lonlat(x=x, y=y, crs=crs)
        else:
            return self._utm_lon(x=x, y=y, utm=crs), self._utm_lat(x=x, y=y, utm=crs)
        
        
    def _pyproj_lonlat(self, x: tuple[float], y: tuple[float], crs: Union[int, str]) -> tuple[np.ndarray, np.ndarray]:
        """Calculates lon and lat coordinates based on given projected x,y-coordinates and the set (or given) CRS projection"""

        # Make pyproj CRS object
        transformer = Transformer.from_crs(crs, CRS.from_epsg(4326), always_xy=True)


        lon, lat = transformer.transform(x, y)

        return np.array(lon), np.array(lat)

    def _lon(self, x: np.ndarray, y: np.ndarray, crs: Optional[Union[int, str]]=None) -> np.ndarray:
        """Calculates lon coordinates based on given projected x,y-coordinates and the set (or given) CRS projection"""
        crs = self.to_crs(crs) or self.crs()
        if not isinstance(self.crs(), tuple) and self.crs().is_engineering:
            return None
        if isinstance(crs, tuple):
            lon = self._utm_lon(x=tuple(x), y=tuple(y), utm=crs)
        else:
            lon, __ = self._lonlat(x=tuple(x), y=tuple(y), crs=crs)
        return lon
    
    def _lat(self, x: np.ndarray, y: np.ndarray, crs: Optional[Union[int, str]]=None) -> np.ndarray:
        """Calculates lat coordinates based on given projected x,y-coordinates and the set (or given) CRS projection"""
        crs = self.to_crs(crs) or self.crs()
        if not isinstance(self.crs(), tuple) and self.crs().is_engineering:
            return None
        if isinstance(crs, tuple):
            lat = self._utm_lat(x=tuple(x), y=tuple(y), utm=crs)
        else:
            __, lat = self._lonlat(x=tuple(x), y=tuple(y), crs=crs)
        return lat
    
    @lru_cache(maxsize=10)
    def _xy(self, lon: tuple[float], lat: tuple[float], crs: Union[int, str]) -> tuple[np.ndarray, np.ndarray]:
        """Calculates projected x and y coordinates based on given lon,lat-coordinates and the set (or given) CRS projection"""
        # Make pyproj CRS object
        transformer = Transformer.from_crs(CRS.from_epsg(4326), self.to_crs(crs), always_xy=True)
        x, y = transformer.transform(lon, lat)

        return np.array(x),np.array(y)

    def _x(self, lon: np.ndarray, lat: np.ndarray, crs: Optional[Union[int, str]]=None) -> np.ndarray:
        """Calculates projected x coordinates based on given lon,lat-coordinates and the set (or given) CRS projection"""
        crs = self.to_crs(crs) or self.utm()
        if not isinstance(self.crs(), tuple) and self.crs().is_engineering:
            logger.log.info("Can't transform lon-lat without a projection!")
            return None
        if isinstance(crs, tuple):
            x = self._utm_x(lon=tuple(lon), lat=tuple(lat), utm=crs)
        else:
            x, __ = self._xy(lon=tuple(lon), lat=tuple(lat), crs=crs)
        return x
    
    def _y(self, lon: np.ndarray, lat: np.ndarray, crs: Optional[Union[int, str]]=None) -> np.ndarray:
        """Calculates projected y coordinates based on given lon,lat-coordinates and the set (or given) CRS projection"""
        crs = self.to_crs(crs) or self.utm()
        if not isinstance(self.crs(), tuple) and self.crs().is_engineering:
            logger.log.info("Can't transform lon-lat without a projection!")
            return None
        if isinstance(crs, tuple):
            y = self._utm_y(lon=tuple(lon), lat=tuple(lat), utm=crs)
        else:
            __, y = self._xy(lon=tuple(lon), lat=tuple(lat), crs=crs)
        return y
    
    @lru_cache(maxsize=10)
    def _utm_lat(self, x: tuple[float], y: tuple[float], utm: tuple[int, str]) -> np.ndarray:
        """Calculates latitudes based on given x,y-coordinates and the set UTM-zone"""
        if not self._is_valid_utm(utm):
            raise ValueError(f"{utm} is not a valid UTM zone!")
        x, y = np.atleast_1d(x), np.atleast_1d(y)
        lat, __ = utm_module.to_latlon(
            x,
            np.mod(y, 10_000_000),
            zone_number=utm[0],
            zone_letter=utm[1],
            strict=False,
        )
        return np.array(lat)

    @lru_cache(maxsize=10)
    def _utm_lon(self, x: tuple[float], y: tuple[float], utm: tuple[int, str]) -> np.ndarray:
        """Calculates longitudes based on given x,y-coordinates and the set UTM-zone"""
        if not self._is_valid_utm(utm):
            raise ValueError(f"{utm} is not a valid UTM zone!")
        x, y = np.atleast_1d(x), np.atleast_1d(y)
        __, lon = utm_module.to_latlon(
            x,
            np.mod(y, 10_000_000),
            zone_number=utm[0],
            zone_letter=utm[1],
            strict=False,
        )
        return np.array(lon)

    @lru_cache(maxsize=10)
    def _utm_x(self, lon: tuple[float], lat: tuple[float], utm: tuple[int, str]) -> np.ndarray:
        """Calculates x-coordinates based on given lon,lat-coordinates and the set UTM-zone.

        latitudes higher than 84 or lower than -80 will produce np.nan"""
        assert len(lon) == len(
            lat
        ), f"lon and lat vectors need to be of equal length ({len(lon)}, {len(lat)})!"
        lat, lon = np.atleast_1d(lat), np.atleast_1d(lon)
        # High/low latitudes cannot be transformed to UTM
        good_mask = np.logical_and(lat <= 84, lat >= -80)
        posmask = np.logical_and(lat >= 0, good_mask)
        negmask = np.logical_and(lat < 0, good_mask)
        x = np.zeros(len(lon))
        if np.any(posmask):
            x[posmask], __, __, __ = utm_module.from_latlon(
                lat[posmask],
                lon[posmask],
                force_zone_number=utm[0],
                force_zone_letter=utm[1],
            )
        if np.any(negmask):
            x[negmask], __, __, __ = utm_module.from_latlon(
                -lat[negmask],
                lon[negmask],
                force_zone_number=utm[0],
                force_zone_letter=utm[1],
            )
        if not np.all(good_mask):
            x[np.logical_not(good_mask)] = np.nan
        return np.array(x)

    @lru_cache(maxsize=10)
    def _utm_y(self, lon: tuple[float], lat: tuple[float], utm: tuple[int, str]) -> np.ndarray:
        """Calculates y-coordinates based on given lon,lat-coordinates and the set UTM-zone.

        latitudes higher than 84 or lower than -80 will produce np.nan"""

        assert len(lon) == len(
            lat
        ), f"lon and lat vectors need to be of equal length ({len(lon)}, {len(lat)})!"
        # lat = cap_lat_for_utm(lat)
        # High/low latitudes cannot be transformed to UTM
        lat, lon = np.atleast_1d(lat), np.atleast_1d(lon)
        good_mask = np.logical_and(lat <= 84, lat >= -80)
        posmask = np.logical_and(lat >= 0, good_mask)
        negmask = np.logical_and(lat < 0, good_mask)
        y = np.zeros(len(lat))

        if np.any(posmask):
            _, y[posmask], __, __ = utm_module.from_latlon(
                lat[posmask],
                lon[posmask],
                force_zone_number=utm[0],
                force_zone_letter=utm[1],
            )
        if np.any(negmask):
            _, y[negmask], __, __ = utm_module.from_latlon(
                -lat[negmask],
                lon[negmask],
                force_zone_number=utm[0],
                force_zone_letter=utm[1],
            )
            y[negmask] = -y[negmask]
        if not np.all(good_mask):
            y[np.logical_not(good_mask)] = np.nan

        return y

    # def _utm_zone_to_crs(self, utm) -> CRS:
    #     proj4 = {
    #         'proj': 'utm',
    #         'zone': utm[0],
    #         'south': utm[1] in SOUTH_UTM_ZONES,  # Use 'south' if in the southern hemisphere
    #         'datum': 'WGS84'
    #     }
    #     return CRS.from_dict(proj4)
    

    def _calculate_rotation_alpha(self, lon, lat, target_crs: CRS) -> np.ndarray:
        """Calculates the rotation angle alpha from lon-lat to the set CRS"""
        saved_alpha = self._alpha.get(target_crs)
        if saved_alpha is not None:
            return saved_alpha
        if isinstance(target_crs, tuple):
            x = self._utm_x(lon=tuple(lon), lat=tuple(lat), utm=target_crs)
            y = self._utm_y(lon=tuple(lon), lat=tuple(lat), utm=target_crs)
            # Shift slightly towards north
            dlat = 1e-5
            x2 = self._utm_x(lon=tuple(lon), lat=tuple(lat+dlat), utm=target_crs)
            y2 = self._utm_y(lon=tuple(lon), lat=tuple(lat+dlat), utm=target_crs)
        else:
            x, y = self._xy(lon=tuple(lon), lat=tuple(lat), crs=target_crs)
            # Shift slightly towards north
            dlat = 1e-5
            x2, y2 = self._xy(lon=tuple(lon), lat=tuple(lat+dlat), crs=target_crs)

        alpha =np.arctan2(y2-y, x2-x)-np.pi/2
        self._alpha[target_crs] = alpha
        return alpha
    
    


    def _rotate_u_v(self, x_data: xr.DataArray, y_data: xr.DataArray, lon: np.ndarray, lat: np.ndarray, grid_shape: tuple[int], target_crs: CRS):
        """Rotates x,y component data to the set coordinate reference system """
        if x_data is None or y_data is None:
            raise ProjectionError(f"Data for both components not found. Cannot rotate!")
        
        if target_crs is None:
            raise ProjectionError(f"No projection given. Cannot rotate!")

        alpha = self._calculate_rotation_alpha(lon, lat, target_crs)
        alpha = _broadcast_alpha_to_data_array(alpha, x_data, y_data, grid_shape)

        x_rot, y_rot = _rotate_components(x_data, y_data, alpha)

        return x_rot, y_rot
    
    
    def _unrotate_u_v(self,  x_data: xr.DataArray, y_data: xr.DataArray, lon: np.ndarray, lat: np.ndarray, grid_shape: tuple[int], source_crs: CRS):
        """*Unrotates x,y component data from the set coordinate reference system """

        if x_data is None or y_data is None:
            raise ProjectionError(f"Data for both components not found. Cannot unrotate!")
        
        if source_crs is None:
            raise ProjectionError(f"No projection given. Cannot unrotate!")

        alpha = self._calculate_rotation_alpha(lon, lat, source_crs)
        alpha = -alpha
        alpha = _broadcast_alpha_to_data_array(alpha, x_data, y_data, grid_shape)

        x_unrot, y_unrot = _rotate_components(x_data, y_data, alpha)

        return x_unrot, y_unrot

    def __repr__(self) -> str:
        return f"ProjManager(crs={self.crs()}, utm={self.utm()})"

def _rotate_components(x_data:xr.DataArray, y_data:xr.DataArray, alpha: xr.DataArray) -> tuple[xr.DataArray, xr.DataArray]:
    """Rotates given components with alpha values. Keeps metadata intact."""
    x_rot =x_data * np.cos(alpha) - y_data * np.sin(alpha)
    y_rot =x_data * np.sin(alpha) + y_data * np.cos(alpha)

    return x_rot, y_rot


def broadcast_alpha_to_target(alpha: np.ndarray, target_shape: tuple[int, ...], copy: bool = False) -> np.ndarray:
    """
    Broadcast `alpha` (numpy array) to `target_shape`.
    - alpha.shape must be a contiguous sub-shape of target_shape.
    - Returns a broadcasted view (read-only) unless copy=True.
    """
    alpha = np.asarray(alpha)
    tshape = tuple(target_shape)
    ashape = tuple(alpha.shape)

    if ashape == tshape:
        return alpha.copy() if copy else alpha

    if len(ashape) > len(tshape):
        raise ValueError(f"alpha has more dimensions {ashape} than target {tshape}")

    # Quick common case: alpha matches the last axes (trailing alignment)
    ndiff = len(tshape) - len(ashape)
    if ashape == tshape[-len(ashape):]:
        newshape = (1,) * ndiff + ashape
        out = np.broadcast_to(alpha.reshape(newshape), tshape)
        return out.copy() if copy else out

    # Try to find a contiguous sub-shape match anywhere in target_shape
    n = len(tshape)
    m = len(ashape)
    for start in range(0, n - m + 1):
        if ashape == tshape[start:start + m]:
            prepend = start
            append = n - m - start
            newshape = (1,) * prepend + ashape + (1,) * append
            out = np.broadcast_to(alpha.reshape(newshape), tshape)
            return out.copy() if copy else out

    raise ValueError(f"alpha.shape {ashape} is not a contiguous sub-shape of target.shape {tshape}")

def _broadcast_alpha_to_data_array(alpha: np.ndarray, x_data: xr.DataArray, y_data:xr.DataArray, grid_shape: tuple[int]) -> xr.DataArray:
    """Broadcastsa a spatial np.ndarray of alpha values to possible time instances also"""

    alpha = np.reshape(alpha, grid_shape)
    alpha = broadcast_alpha_to_target(alpha, target_shape=x_data.shape)
    return alpha




# reuse allowlist from before (adjust as needed)
_CF_ALLOWLIST = {
    "grid_mapping_name",
    "longitude_of_central_meridian",
    "latitude_of_projection_origin",
    "standard_parallel",
    "standard_parallel_1", "standard_parallel_2",
    "false_easting", "false_northing",
    "scale_factor_at_central_meridian",
    "latitude_of_origin",
    "longitude_of_center",
    "azimuth",
    "grid_north_pole_longitude", "grid_north_pole_latitude",
    "semi_major_axis", "inverse_flattening",
    "towgs84",
}


def _is_utm_tuple(obj) -> bool:
    """Return True if obj looks like a UTM tuple (zone:int, band:str)."""
    if not isinstance(obj, tuple) or len(obj) != 2:
        return False
    zone, band = obj
    if not isinstance(zone, int):
        return False
    if not (isinstance(band, str) and len(band.strip()) == 1 and band.strip().isalpha()):
        return False
    return True


def _validate_utm_zone_and_band(zone: int, band: str) -> tuple[int, str]:
    """Validate and normalize zone and band. Raises ValueError on bad input."""
    if not (1 <= zone <= 60):
        raise ValueError(f"UTM zone number out of range: {zone}")
    band_up = band.strip().upper()
    # UTM latitude bands are letters C..X excluding I and O
    if band_up < "C" or band_up > "X" or band_up in {"I", "O"}:
        raise ValueError(f"Invalid UTM latitude band letter: {band!r}")
    return zone, band_up


def _band_to_hemisphere(band_letter: str) -> str:
    """Return 'N' for northern-hemisphere bands (N..X) otherwise 'S'."""
    # Bands N .. X are northern hemisphere; C..M are southern
    return "N" if band_letter >= "N" else "S"


def crs_to_netcdf_attrs(crs_input) -> dict[str, Any]:
    """
    Return a compact dict of attributes suitable for writing to a CF grid-mapping
    variable. Handles:
      - pyproj.CRS or any input accepted by CRS.from_user_input
      - explicit UTM tuple (zone:int, band:str) — preserves the latitude band
      - engineering/local CRSs (is_engineering)
    """
    if crs_input is None:
        return {}

    # 1) If explicit UTM tuple like (33, 'W')
    if _is_utm_tuple(crs_input):
        try:
            zone_raw, band_raw = crs_input
            zone, band = _validate_utm_zone_and_band(zone_raw, band_raw)
        except ValueError as e:
            # invalid tuple: return empty or raise depending on your policy
            raise

        hemisphere = _band_to_hemisphere(band)
        attrs: dict[str, Any] = {
            "grid_mapping_name": "transverse_mercator",  # CF uses transverse_mercator for UTM
            "utm_zone_number": int(zone),
            "utm_latitude_band": band,                   # preserve exact band letter!
            "utm_hemisphere": hemisphere,                # 'N' or 'S'
            # human-friendly note
            "description": (
                f"UTM zone {zone}{band} (latitude band {band}); "
                f"Hemisphere: {hemisphere}."
            ),
        }

        # # Suggest an EPSG code for the zone+hemisphere (but do not drop the band)
        # try:
        #     epsg_code = 32600 + zone if hemisphere == "N" else 32700 + zone
        #     attrs["epsg_code_candidate"] = f"EPSG:{epsg_code}"

        #     # Optionally populate more CF fields from the EPSG CRS if pyproj can create it.
        #     try:
        #         crs_epsg = CRS.from_epsg(epsg_code)
        #         # extract a compact set of CF-attrs from the EPSG CRS
        #         try:
        #             cf_dict = crs_epsg.to_cf()
        #         except Exception:
        #             cf_dict = {}
        #         for k, v in cf_dict.items():
        #             if k in _CF_ALLOWLIST and k not in attrs:
        #                 attrs[k] = v
        #         # also include precise WKT if available
        #         try:
        #             attrs["crs_wkt"] = crs_epsg.to_wkt()
        #             attrs["spatial_ref"] = attrs["crs_wkt"]
        #         except Exception:
        #             pass
        #     except CRSError:
        #         # can't materialize EPSG CRS: that's OK — we still preserved the band
        #         pass
        # except Exception:
        #     pass

        return attrs

    # 2) Otherwise try normal pyproj CRS handling (CRS, epsg code, WKT, etc.)
    try:
        crs = crs_input if isinstance(crs_input, CRS) else CRS.from_user_input(crs_input)
    except CRSError:
        return {}

    # engineering/local
    if getattr(crs, "is_engineering", False):
        try:
            wkt = crs.to_wkt()
        except Exception:
            wkt = (
                'ENGINEERINGCRS["Local Cartesian", ENGINEERINGDATUM["Local datum"], '
                'CS[Cartesian,2], AXIS["x",east], AXIS["y",north], LENGTHUNIT["metre",1]]'
            )
        return {
            "grid_mapping_name": "local_cartesian",
            "crs_wkt": wkt,
            "spatial_ref": wkt,
            "units": "metre",
            "description": "Local engineering Cartesian CRS (no geodetic tie).",
        }

    # non-engineering: filter pyproj.to_cf()
    attrs: dict[str, Any] = {}
    try:
        cf_dict = crs.to_cf()
    except Exception:
        cf_dict = {}
    for k, v in cf_dict.items():
        if k in _CF_ALLOWLIST:
            attrs[k] = v

    # authority / epsg
    try:
        auth = crs.to_authority()
    except Exception:
        auth = None
    if auth:
        attrs["epsg_code"] = f"{auth[0]}:{auth[1]}"
    else:
        try:
            epsg = crs.to_epsg()
            if epsg:
                attrs["epsg_code"] = f"EPSG:{epsg}"
        except Exception:
            pass

    # add WKT / spatial_ref
    try:
        wkt = crs.to_wkt()
        attrs["crs_wkt"] = wkt
        attrs["spatial_ref"] = wkt
    except Exception:
        try:
            proj4 = crs.to_proj4()
            if proj4:
                attrs["proj4"] = proj4
        except Exception:
            pass

    # ellipsoid numeric info
    try:
        ell = crs.ellipsoid
        sa = getattr(ell, "semi_major_metre", None) or getattr(ell, "semi_major_axis", None)
        if sa:
            attrs.setdefault("semi_major_axis", float(sa))
        invf = getattr(ell, "inverse_flattening", None)
        if invf:
            attrs.setdefault("inverse_flattening", float(invf))
    except Exception:
        pass

    return attrs