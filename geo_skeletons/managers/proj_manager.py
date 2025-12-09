from .metadata_manager import MetaDataManager
from typing import Optional, Union
from pyproj import CRS
class ProjManager:
    def __init__(
        self, metadata_manager: MetaDataManager, crs: Optional[Union[int, str]]=None,
    ):
        self._epsg = None
        self._proj4 = None
        self.set(crs)
        self._meta: MetaDataManager = metadata_manager


    def epsg(self) -> Union[int, None]:
        return self._epsg
    
    def proj4(self) -> Union[str, None]:
        return self._proj4
    
    def set(self, crs: Union[int, str], silent: bool = False) -> None:
        """Sets the CRS (Coordinate reference system) based on eithern an EPSG code [int] or a proj4 string [str]. A string 'EPSG:4326' will be docoded to 4326."""
        if crs is None:
            return 
        if isinstance(crs, int):
            self._epsg = crs
            self._proj4 = None
            if not silent:
                print(f"Setting EPSG code {self._epsg}")
            return


        if not isinstance(crs, str):
            raise ValueError(f"crs needs to be of type 'int' or 'str', not {crs}!")

        if crs[0:4] == 'EPSG':
            self._epsg = int(crs[5:])
            self._proj4 = None
            if not silent:
                print(f"Setting EPSG code {self._epsg}")
            return
        
        # Assume that crs is a proj4 string by now
        self._proj4 = crs
        self._epsg = None
        if not silent:
            print(f"Setting proj4 string {self._proj4}")

    def crs(self) -> Union[CRS, None]:
        if self._epsg is not None:
            return CRS.from_epsg(self._epsg)
        elif self._proj4 is not None:
            return CRS.from_proj4(self._proj4)
        else:
            return None