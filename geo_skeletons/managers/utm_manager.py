from .metadata_manager import MetaDataManager
import numpy as np
import numpy as np
import utm as utm_module
from typing import Optional

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


class UTMManager:
    def __init__(
        self, lat: tuple[float], lon: tuple[float], metadata_manager: MetaDataManager
    ):
        self._zone: tuple[int, str] = (None, None)
        self._lat_edges: float = lat
        self._lon_edges: float = lon
        self._meta: MetaDataManager = metadata_manager

    def zone(self) -> tuple[int, str]:
        """Returns UTM zone number and letter. Returns (None, None)
        if it hasn't been set by the user in cartesian grids."""
        return self._zone

    def is_valid(self, utm: tuple[int, str]) -> bool:
        """Checks that the given utm zone is valid"""
        if len(utm) != 2:
            return False
        if not utm[0] in VALID_UTM_NUMBERS:
            return False
        if not utm[1] in VALID_UTM_ZONES:
            return False
        return True

    def is_set(self) -> bool:
        """Checks if the UTM zone has been set"""
        return not self._zone == (None, None)

    def optimal_utm(self, lon: np.ndarray, lat: np.ndarray) -> tuple[int, str]:
        """Determines an optimat UTM-zone given longitude and latitude coordinates."""
        try:
            __, __, zone_number, zone_letter = utm_module.from_latlon(
                np.array(lat), np.array(lon)
            )
        except ValueError:  # ValueError: latitudes must all have the same sign
            __, __, zone_number, zone_letter = utm_module.from_latlon(
                np.median(np.array(lat)), np.median(np.array(lon))
            )
        return (zone_number, zone_letter)

    def reset(self, silent: bool = False) -> None:
        """Resets the UTM-zone based on the lon/lat edges"""
        if self._lat_edges[0] is None:
            self._zone = (None, None)
        else:
            lon = self._lon_edges
            lat = np.minimum(np.maximum(self._lat_edges, -80), 84)
            # *** utm.error.OutOfRangeError: latitude out of range (must be between 80 deg S and 84 deg N)
            # raise OutOfRangeError('longitude out of range (must be between 180 deg W and 180 deg E)')
            self._zone = self.optimal_utm(lon=lon, lat=lat)
        if not silent:
            print(f"Setting UTM {self._zone}")

    def set(self, zone: Optional[tuple[int, str]], silent: bool = False) -> None:
        """Set UTM zone and number to be used for cartesian coordinates.

        If 'None' for a spherical grid, they will be deduced.
        """
        if zone is None:
            self.reset(silent=silent)
            return

        if zone == (None, None):
            self.reset(silent=silent)
            return

        if not self.is_valid(zone):
            raise ValueError(f"{zone} is not a valid UTM zone!")

        self._zone = (zone[0], zone[1])
        self._meta.append({"utm_zone": f"{zone[0]:02.0f}{zone[1]}"})

        if not silent:
            print(f"Setting UTM {self._zone}")

    def _lat(self, x: np.ndarray, y: np.ndarray) -> np.ndarray:
        """Calculates latitudes based on given x,y-coordinates and the set UTM-zone"""
        if self._zone[0] is None:
            print("Need to set an UTM-zone, e.g. set_utm((33,'W')), to get latitudes!")
            return None

        if not self.is_valid(self._zone):
            raise ValueError(f"{self._zone} is not a valid UTM zone!")
        lat, __ = utm_module.to_latlon(
            x,
            np.mod(y, 10_000_000),
            zone_number=self._zone[0],
            zone_letter=self._zone[1],
            strict=False,
        )
        return lat

    def _lon(self, x: np.ndarray, y: np.ndarray) -> np.ndarray:
        """Calculates longitudes based on given x,y-coordinates and the set UTM-zone"""
        if self._zone[0] is None:
            print("Need to set an UTM-zone, e.g. set_utm((33,'W')), to get longitudes!")
            return None

        if not self.is_valid(self._zone):
            raise ValueError(f"{self._zone} is not a valid UTM zone!")

        __, lon = utm_module.to_latlon(
            x,
            np.mod(y, 10_000_000),
            zone_number=self._zone[0],
            zone_letter=self._zone[1],
            strict=False,
        )
        return lon

    def _x(self, lon: np.ndarray, lat: np.ndarray, utm: tuple[int, str]) -> np.ndarray:
        """Calculates x-coordinates based on given lon,lat-coordinates and the set UTM-zone.

        latitudes higher than 84 or lower than -80 will produce np.nan"""
        assert len(lon) == len(
            lat
        ), f"lon and lat vectors need to be of equal length ({len(lon)}, {len(lat)})!"
        utm = utm or self._zone
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

    def _y(self, lon: np.ndarray, lat: np.ndarray, utm: tuple[int, str]) -> np.ndarray:
        """Calculates x-coordinates based on given lon,lat-coordinates and the set UTM-zone.

        latitudes higher than 84 or lower than -80 will produce np.nan"""

        assert len(lon) == len(
            lat
        ), f"lon and lat vectors need to be of equal length ({len(lon)}, {len(lat)})!"
        utm = utm or self._zone
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


# def cap_lat_for_utm(lat):
#     if isinstance(lat, float):
#         lat = np.array([lat])
#     if len(lat) > 0 and max(lat) > 84:
#         print(
#             f"Max latitude {max(lat)}>84. These points well be capped to 84 deg in UTM conversion!"
#         )
#         lat[lat > 84.0] = 84.0
#     if len(lat) > 0 and min(lat) < -80:
#         lat[lat < -80.0] = -80.0
#         print(
#             f"Min latitude {min(lat)}<-80. These points well be capped to -80 deg in UTM conversion!"
#         )
#     return lat
