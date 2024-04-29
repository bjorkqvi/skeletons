import numpy as np


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


def valid_utm_zone(utm_zone: tuple[int, str]) -> bool:
    """Checks that a UTM zone, e.g. (33, 'V') is valid. (None, None) is excepted."""

    zone_number, zone_letter = utm_zone

    if zone_number is None and zone_letter is None:
        return True

    if zone_number not in VALID_UTM_NUMBERS:
        return False

    if zone_letter not in VALID_UTM_ZONES:
        return False

    return True


def cap_lat_for_utm(lat):
    if isinstance(lat, float):
        lat = np.array([lat])
    if len(lat) > 0 and max(lat) > 84:
        print(
            f"Max latitude {max(lat)}>84. These points well be capped to 84 deg in UTM conversion!"
        )
        lat[lat > 84.0] = 84.0
    if len(lat) > 0 and min(lat) < -80:
        lat[lat < -80.0] = -80.0
        print(
            f"Min latitude {min(lat)}<-80. These points well be capped to -80 deg in UTM conversion!"
        )
    return lat
