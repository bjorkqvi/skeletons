from .metadata_manager import MetaDataManager
from typing import Optional, Union
from pyproj import CRS, Transformer
import numpy as np
import utm as utm_module
import xarray as xr
from geo_skeletons.errors import ProjectionError
from geo_parameters.metaparameter import MetaParameter
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

SOUTH_UTM_ZONES = ["C", "D", "E", "F", "G", "H", "J", "K", "L", "M"]

VALID_UTM_NUMBERS = np.linspace(1, 60, 60).astype(int)

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

class ProjManager:
    def __init__(
        self,  lat: tuple[float], lon: tuple[float],y: tuple[float], x: tuple[float], metadata_manager: MetaDataManager, crs: Optional[Union[int, str]]=None,
    ):
        self._lat_edges: float = lat
        self._lon_edges: float = lon        
        self._y_edges: float = y
        self._x_edges: float = x 
        self._meta: MetaDataManager = metadata_manager
        self._crs = None
        if crs is not None:
            self.set(crs, silent=True)
        

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
        if np.logical_not(np.all(mask)):
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

    def my_utm(self, optimal: bool = False) -> Union[tuple[int, str], None]:
        """Returns the UTM (Universal Transverse Mercator) zone for the grid.

        1) If the UTM zone is already set and `optimal=False`, it returns the currently set UTM zone. 
        2) If UTM is not set or `optimal=True` it calculates and returns the optimal UTM zone based on the grid's 
        longitude and latitude edges. 
        3) If no UTM zone is set and the grid is undefined, it returns `None`.

        Args:
            optimal (bool, optional): If `True`, calculates the optimal UTM zone even if 
                another UTM zone is already set. Defaults to `False`.

        Returns:
            Optional[tuple]: The UTM zone as a tuple (e.g., `(33, 'N')`) if determined, 
            or `None` if no UTM zone can be calculated.
        """
        if isinstance(self._crs, tuple) and not optimal:
            return self._crs
        if self.crs() is None:
            return None
        
        if self._lon_edges == (None, None):
            lon, lat = self._lonlat(self._x_edges, self._y_edges, self.crs())
        else:
            lon, lat = self._lon_edges, self._lat_edges
        return self._optimal_utm(lon, lat)

    def reset_utm(self, silent: bool = False) -> None:
        """Resets the UTM zone based on the grid's longitude and latitude edges.

        This method recalculates the UTM zone using the current longitude and latitude 
        edges of the grid. The latitude edges are capped to above -80° and below 84°).

        Args:
            silent (bool, optional): If `True`, suppresses the print statement that displays 
                the new UTM zone after resetting. Defaults to `False`.

        Returns:
            None: This method modifies the object's UTM zone in place.
        """

        if self._lat_edges[0] is None:
            self._crs = None
        else:
            lon = self._lon_edges
            lat = np.minimum(np.maximum(self._lat_edges, -80), 84)
            # *** utm.error.OutOfRangeError: latitude out of range (must be between 80 deg S and 84 deg N)
            # raise OutOfRangeError('longitude out of range (must be between 180 deg W and 180 deg E)')
            self._crs = self._optimal_utm(lon=lon, lat=lat)
        if not silent and self._crs is not None:
            print(f"Setting UTM {self._crs}")

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
        if self.crs() is None:
            return False
        return self.crs().is_geographic
    
    def set(self, crs: CRSValue, silent: bool = True) -> None:
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
            silent (bool, optional): If `True`, suppresses print statements that indicate 
                the CRS being set. Defaults to `True`.

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
        epsg, proj4, cf_dict, utm, crs_obj = decode_crs(crs)
        
        if epsg is not None:
            if not silent:
                print(f"Setting EPSG code {epsg}")
            if self._meta is not None:
                self._meta.set({'epsg':epsg},'crs')
        elif proj4 is not None:
            if not silent:
                print(f"Setting proj4 string {proj4}")
            if self._meta is not None:
                self._meta.set({'proj4':proj4},'crs')
        elif cf_dict is not None:
            if not silent:
                print(f"Setting projection from dict {cf_dict}")
            if self._meta is not None:
                self._meta.set(cf_dict,'crs')
        elif utm is not None:
            if not silent:
                print(f"Setting UTM {utm}")
            if self._meta is not None:
                self._meta.set({"utm_zone": utm[0], 'utm_letter': utm[1]},'crs')
        elif crs_obj is not None:
            if not silent:
                print(f"Setting CRS {crs_obj}")
            if self._meta is not None:
                try:
                    self._meta.set(crs.to_cf(),'crs')
                except KeyError: # *** KeyError: 'o_lon_p'
                    self._meta.set(crs.to_dict(),'crs')
        else:
            raise ValueError(f"{crs} is not a valid coordinate reference system!")
        
        self._crs = crs
        # Set correct units for variables x and y if we are in a rotated grid
        if self.units_are_in_degrees():
            self._meta.set({'units': 'degrees'},'x')
            self._meta.set({'units': 'degrees'},'y')

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


    def _lonlat(self, x: np.ndarray, y: np.ndarray, crs: Union[int, str]) -> tuple[np.ndarray, np.ndarray]:
        """Calculates lon and lat coordinates based on given projected x,y-coordinates and the set (or given) CRS projection"""

        # Make pyproj CRS object
        transformer = Transformer.from_crs(crs, CRS.from_epsg(4326), always_xy=True)


        lon, lat = transformer.transform(x, y)

        return lon, lat

    def _lon(self, x: np.ndarray, y: np.ndarray, crs: Optional[Union[int, str]]=None) -> np.ndarray:
        """Calculates lon coordinates based on given projected x,y-coordinates and the set (or given) CRS projection"""
        crs = self.to_crs(crs) or self.crs()
        if crs is None:
            #print("Can't transform x-y without a projection!")
            return None
        if isinstance(crs, tuple):
            lon = self._utm_lon(x=x, y=y, utm=crs)
        else:
            lon, __ = self._lonlat(x=x, y=y, crs=crs)
        return lon
    
    def _lat(self, x: np.ndarray, y: np.ndarray, crs: Optional[Union[int, str]]=None) -> np.ndarray:
        """Calculates lat coordinates based on given projected x,y-coordinates and the set (or given) CRS projection"""
        crs = self.to_crs(crs) or self.crs()
        if crs is None:
            #print("Can't transform x-y without a projection!")
            return None
        if isinstance(crs, tuple):
            lat = self._utm_lat(x=x, y=y, utm=crs)
        else:
            __, lat = self._lonlat(x=x, y=y, crs=crs)
        return lat
    

    def _xy(self, lon: np.ndarray, lat: np.ndarray, crs: Union[int, str]) -> tuple[np.ndarray, np.ndarray]:
        """Calculates projected x and y coordinates based on given lon,lat-coordinates and the set (or given) CRS projection"""
        # Make pyproj CRS object
        transformer = Transformer.from_crs(CRS.from_epsg(4326), crs, always_xy=True)
        x, y = transformer.transform(lon, lat)

        return x,y

    def _x(self, lon: np.ndarray, lat: np.ndarray, crs: Optional[Union[int, str]]=None) -> np.ndarray:
        """Calculates projected x coordinates based on given lon,lat-coordinates and the set (or given) CRS projection"""
        crs = self.to_crs(crs) or self.crs()
        if crs is None:
            print("Can't transform lon-lat without a projection!")
            return None
        if isinstance(crs, tuple):
            x = self._utm_x(lon=lon, lat=lat, utm=crs)
        else:
            x, __ = self._xy(lon=lon, lat=lat, crs=crs)
        return x
    
    def _y(self, lon: np.ndarray, lat: np.ndarray, crs: Optional[Union[int, str]]=None) -> np.ndarray:
        """Calculates projected y coordinates based on given lon,lat-coordinates and the set (or given) CRS projection"""
        crs = self.to_crs(crs) or self.crs()
        if crs is None:
            print("Can't transform lon-lat without a projection!")
            return None
        if isinstance(crs, tuple):
            y = self._utm_y(lon=lon, lat=lat, utm=crs)
        else:
            __, y = self._xy(lon=lon, lat=lat, crs=crs)
        return y
    

    def _utm_lat(self, x: np.ndarray, y: np.ndarray, utm: tuple[int, str]) -> np.ndarray:
        """Calculates latitudes based on given x,y-coordinates and the set UTM-zone"""
        if not self._is_valid_utm(self.crs()):
            raise ValueError(f"{self.crs()} is not a valid UTM zone!")
        lat, __ = utm_module.to_latlon(
            x,
            np.mod(y, 10_000_000),
            zone_number=utm[0],
            zone_letter=utm[1],
            strict=False,
        )
        return lat

    def _utm_lon(self, x: np.ndarray, y: np.ndarray, utm: tuple[int, str]) -> np.ndarray:
        """Calculates longitudes based on given x,y-coordinates and the set UTM-zone"""
        if not self._is_valid_utm(self.crs()):
            raise ValueError(f"{self.crs()} is not a valid UTM zone!")

        __, lon = utm_module.to_latlon(
            x,
            np.mod(y, 10_000_000),
            zone_number=utm[0],
            zone_letter=utm[1],
            strict=False,
        )
        return lon

    def _utm_x(self, lon: np.ndarray, lat: np.ndarray, utm: tuple[int, str]) -> np.ndarray:
        """Calculates x-coordinates based on given lon,lat-coordinates and the set UTM-zone.

        latitudes higher than 84 or lower than -80 will produce np.nan"""
        assert len(lon) == len(
            lat
        ), f"lon and lat vectors need to be of equal length ({len(lon)}, {len(lat)})!"
        # lat = cap_lat_for_utm(lat)
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
        return x

    def _utm_y(self, lon: np.ndarray, lat: np.ndarray, utm: tuple[int, str]) -> np.ndarray:
        """Calculates x-coordinates based on given lon,lat-coordinates and the set UTM-zone.

        latitudes higher than 84 or lower than -80 will produce np.nan"""

        assert len(lon) == len(
            lat
        ), f"lon and lat vectors need to be of equal length ({len(lon)}, {len(lat)})!"
        # lat = cap_lat_for_utm(lat)
        # High/low latitudes cannot be transformed to UTM
        good_mask = np.logical_and(lat <= 84, lat >= -80)
        lon = np.atleast_1d(lon)
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

    def _utm_zone_to_crs(self, utm) -> CRS:
        proj4 = {
            'proj': 'utm',
            'zone': utm[0],
            'south': utm[1] in SOUTH_UTM_ZONES,  # Use 'south' if in the southern hemisphere
            'datum': 'WGS84'
        }
        return CRS.from_dict(proj4)

    def _rotate_u_v(self, x_data, y_data, lon: np.ndarray, lat: np.ndarray, grid_shape: tuple[int]):
        """Rotates x,y component data to the set coordinate reference system """
        if x_data is None or y_data is None:
            raise ProjectionError(f"Data for both components not found. Cannot rotate!")
        
        if self.crs() is None:
            raise ProjectionError(f"No projection defined. Cannot rotate!")

        if isinstance(self.crs(), tuple):
            x = self._utm_x(lon=lon, lat=lat, utm=self.crs())
            y = self._utm_y(lon=lon, lat=lat, utm=self.crs())
            # Shift slightly towards north
            dlat = 1e-5
            x2 = self._utm_x(lon=lon, lat=lat+dlat, utm=self.crs())
            y2 = self._utm_y(lon=lon, lat=lat+dlat, utm=self.crs())
        else:
            x, y = self._xy(lon=lon, lat=lat, crs=self.crs())
            # Shift slightly towards north
            dlat = 1e-5
            x2, y2 = self._xy(lon=lon, lat=lat+dlat, crs=self.crs())

        alpha =np.arctan2(y2-y, x2-x)-np.pi/2
        
        if hasattr(x_data, 'lon'):
            x_str, y_str = 'lon', 'lat'
        elif hasattr(x_data, 'x'):
            x_str, y_str = 'x', 'y'
        else:
            x_str, y_str = 'inds', None

        alpha = np.reshape(alpha, grid_shape)
        # Redoing this to a DataArray automatically broadcasts alpha values to different time steps
        if y_str is not None:
            alpha = xr.DataArray(data=alpha,dims=[y_str, x_str], coords={x_str: ([x_str], x_data[x_str].values), y_str: ([y_str], y_data[y_str].values)})
        else:
            alpha = xr.DataArray(data=alpha,dims=[x_str], coords={x_str: ([x_str], x_data[x_str].values)})

        # To preserve metadata
        xmeta = x_data.attrs
        ymeta = y_data.attrs
        
        x_rot =x_data * np.cos(alpha) - y_data * np.sin(alpha)
        y_rot =x_data * np.sin(alpha) + y_data * np.cos(alpha)

        x_rot = x_rot.assign_attrs(xmeta)
        y_rot = y_rot.assign_attrs(ymeta)
        return x_rot, y_rot
