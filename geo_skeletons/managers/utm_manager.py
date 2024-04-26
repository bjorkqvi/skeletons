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
        if zone is None or zone[0] is None:
            self.reset()
            return

        zone_number, zone_letter = zone

        if isinstance(zone_number, int) or isinstance(zone_number, float):
            zone_number = int(zone_number)
        else:
            raise ValueError(f"'zone' needs to be tuple[str,int], not {zone}!")

        if isinstance(zone_letter, str):
            zone_letter = str(zone_letter)
        else:
            raise ValueError(f"'zone' needs to be tuple[str,int], not {zone}!")

        if not zone_number in VALID_UTM_NUMBERS or not zone_letter in VALID_UTM_ZONES:
            raise ValueError(f"{zone} is not a valid UTM zone!")

        self._zone = (zone_number, zone_letter)
        self._meta.append({"utm_zone": f"{zone_number:02.0f}{zone_letter}"})

        if not silent:
            print(f"Setting UTM {self._zone}")
