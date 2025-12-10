from .metadata_manager import MetaDataManager
from typing import Optional, Union
from pyproj import CRS, Transformer
import numpy as np
import utm as utm_module
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

VALID_UTM_NUMBERS = np.linspace(1, 60, 60).astype(int)


def decode_crs(crs:Optional[Union[str, int]]=None) -> tuple[int, str, dict]:
    """Decodes the give crs hat can be either EPSG code (int) or roj4 string (str) to an EPSG code and proj4 string (and None)"""
    if crs is None:
        return None, None, None, None, None
    
    if isinstance(crs, CRS):
        return None, None, None, None, crs
    
    if isinstance(crs, dict):
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
        self,  lat: tuple[float], lon: tuple[float], metadata_manager: MetaDataManager, crs: Optional[Union[int, str]]=None,
    ):
        self._lat_edges: float = lat
        self._lon_edges: float = lon        
        self._meta: MetaDataManager = metadata_manager
        self._crs = None
        if crs is not None:
            self.set(crs)
        

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

    def reset_utm(self, silent: bool = False) -> None:
        """Resets the UTM-zone based on the lon/lat edges"""

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

    def set(self, crs: Union[int, str], silent: bool = False) -> None:
        """Sets the CRS (Coordinate reference system) based on eithern an EPSG code [int] or a proj4 string [str]. A string 'EPSG:4326' will be docoded to 4326."""

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
        

    def to_crs(self, crs: Optional[Union[str, int, dict]]=None) -> Union[CRS, tuple[int, str], None]:
        """Return a pyproj CRS object for the given crs that can be either EPSG code (int), proj4 string (str) or dict"""
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
        """Returns the pyproj CRS object or UTM tuple representing the set projection """
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
            print("Can't transform x-y without a projection!")
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
            print("Can't transform x-y without a projection!")
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

