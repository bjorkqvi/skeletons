from .metadata_manager import MetaDataManager
import numpy as np
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


class UTMManager:
    def __init__(
        self, lat: tuple[float], lon: tuple[float], metadata_manager: MetaDataManager
    ):
        self._zone: tuple[int, str] = (None, None)
        self._lat: float = lat
        self._lon: float = lon
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

    def reset(self) -> None:
        if self._lat[0] is None:
            zone_number, zone_letter = (None, None)
        else:
            lat = np.mean(self._lat)
            lon = np.mean(self._lon)
            lat = np.minimum(np.maximum(lat, -80), 84)
            # *** utm.error.OutOfRangeError: latitude out of range (must be between 80 deg S and 84 deg N)
            # raise OutOfRangeError('longitude out of range (must be between 180 deg W and 180 deg E)')

            __, __, zone_number, zone_letter = utm_module.from_latlon(lat, lon)

        self._zone = (zone_number, zone_letter)
        print(f"Setting UTM {self._zone}")

    def set(self, zone: tuple[int, str], silent: bool = False) -> None:
        """Set UTM zone and number to be used for cartesian coordinates.

        If not given for a spherical grid, they will be deduced.
        """
        if zone is None:
            self.reset()
            return

        if zone == (None, None):
            self.reset()
            return

        if not self.is_valid(zone):
            raise ValueError(f"{zone} is not a valid UTM zone!")

        self._zone = (zone[0], zone[1])
        self._meta.append({"utm_zone": f"{zone[0]:02.0f}{zone[1]}"})

        if not silent:
            print(f"Setting UTM {self._zone}")

    # def _lat(self, x: np.ndarray, y: np.ndarray) -> np.ndarray:
    #     lat, __ = utm_module.to_latlon(
    #         x,
    #         np.mod(y, 10_000_000),
    #         zone_number=self._zone[0],
    #         zone_letter=self._zone[1],
    #         strict=False,
    #     )
    #     return lat
