from .metadata_manager import MetaDataManager
from typing import Optional, Union
from pyproj import CRS, Transformer
import numpy as np


def decode_crs(crs:Optional[Union[str, int]]=None) -> tuple[int, str]:
    """Decodes the give crs hat can be either EPSG code (int) or roj4 string (str) to an EPSG code and proj4 string (and None)"""
    if crs is None:
        return None, None
    if isinstance(crs, int):
        return crs, None
    
    if not isinstance(crs, str):
        raise ValueError(f"crs needs to be of type 'int' or 'str', not {crs}!")

    if crs[0:4] == 'EPSG':
        return int(crs[5:]), None
    
    return None, crs

class ProjManager:
    def __init__(
        self, metadata_manager: MetaDataManager, crs: Optional[Union[int, str]]=None,
    ):
        self.set(crs)
        self._meta: MetaDataManager = metadata_manager


    def epsg(self) -> Union[int, None]:
        return self._epsg
    
    def proj4(self) -> Union[str, None]:
        return self._proj4
    


    def set(self, crs: Union[int, str], silent: bool = False) -> None:
        """Sets the CRS (Coordinate reference system) based on eithern an EPSG code [int] or a proj4 string [str]. A string 'EPSG:4326' will be docoded to 4326."""
        epsg, proj4 = decode_crs(crs)
        
        if epsg is not None:
            if not silent:
                print(f"Setting EPSG code {epsg}")
        if proj4 is not None:
            if not silent:
                print(f"Setting proj4 string {proj4}")   

        self._epsg = epsg
        self._proj4 = proj4
        


    def crs(self, crs: Optional[Union[str, int]]=None) -> Union[CRS, None]:
        """Return a pyproj CRS object for the set crs, or the given crs that can be either EPSG code (int) or roj4 string (str)"""
        epsg, proj4 = decode_crs(crs)

        epsg = epsg or self._epsg
        proj4 = proj4 or self._proj4
        
        if epsg is not None:
            return CRS.from_epsg(epsg)
        elif proj4 is not None:
            return CRS.from_proj4(proj4)
        else:
            return None
        

    def _lonlat(self, x: np.ndarray, y: np.ndarray, crs: Optional[Union[int, str]]=None) -> tuple[np.ndarray, np.ndarray]:
        """Calculates lon and lat coordinates based on given projected x,y-coordinates and the set (or given) CRS projection"""

        # Make pyproj CRS object
        crs = self.crs(crs)
        transformer = Transformer.from_crs(crs, CRS.from_epsg(4326), always_xy=True)


        lon, lat = transformer.transform(x, y)

        return lon, lat

    def _lon(self, x: np.ndarray, y: np.ndarray, crs: Optional[Union[int, str]]=None) -> np.ndarray:
        """Calculates lon coordinates based on given projected x,y-coordinates and the set (or given) CRS projection"""
        lon, __ = self._lonlat(x=x, y=y, crs=crs)
        return lon
    
    def _lat(self, x: np.ndarray, y: np.ndarray, crs: Optional[Union[int, str]]=None) -> np.ndarray:
        """Calculates lat coordinates based on given projected x,y-coordinates and the set (or given) CRS projection"""
        __, lat = self._lonlat(x=x, y=y, crs=crs)
        return lat
    

    def _xy(self, lon: np.ndarray, lat: np.ndarray, crs: Optional[Union[int, str]]=None) -> tuple[np.ndarray, np.ndarray]:
        """Calculates projected x and y coordinates based on given lon,lat-coordinates and the set (or given) CRS projection"""
        # Make pyproj CRS object
        crs = self.crs(crs)
        transformer = Transformer.from_crs(CRS.from_epsg(4326), crs, always_xy=True)
        x, y = transformer.transform(lon, lat)

        return x,y

    def _x(self, lon: np.ndarray, lat: np.ndarray, crs: Optional[Union[int, str]]=None) -> np.ndarray:
        """Calculates projected x coordinates based on given lon,lat-coordinates and the set (or given) CRS projection"""
        x, __ = self._lonlat(lon=lon, lat=lat, crs=crs)
        return x
    
    def _lat(self, lon: np.ndarray, lat: np.ndarray, crs: Optional[Union[int, str]]=None) -> np.ndarray:
        """Calculates projected y coordinates based on given lon,lat-coordinates and the set (or given) CRS projection"""
        __, y = self._lonlat(lon=lon, lat=lat, crs=crs)
        return y