import geopy.distance
import numpy as np


def min_distance(
    lon: float, lat: float, lon_vec: np.ndarray, lat_vec: np.ndarray
) -> tuple[float, int]:
    """Calculates minimum distance [m] between a given point and a list of
    points given in spherical coordinates (lon/lat degrees).

    Also returns index of the found minimum.
    """
    dx = []
    for n, __ in enumerate(lat_vec):
        dx.append(distance_2points(lat, lon, lat_vec[n], lon_vec[n]))

    return np.array(dx).min(), np.array(dx).argmin()


def min_cartesian_distance(
    x: float, y: float, x_vec: np.ndarray, y_vec: np.ndarray
) -> tuple[float, int]:
    """ "Calculates minimum distance [m] between a given point and list of points given
    in cartesian coordinates [m].

    Also returns incex of found minimum"""
    dx = ((y - y_vec) ** 2 + (x - x_vec) ** 2) ** 0.5

    return dx.min(), dx.argmin()


def lon_in_km(lat: float) -> float:
    """Converts one longitude degree to km for a given latitude."""
    return distance_2points(lat, 0, lat, 1) / 1000


def lat_in_km(lat: float) -> float:
    """Converts one latitude degree to km for a given latitude."""
    return distance_2points(lat, 0, lat + 1, 0) / 1000


def domain_size_in_km(
    lon: tuple[float, float], lat: tuple[float, float]
) -> tuple[float, float]:
    """Calculates approximate size of grid in km."""
    km_x = (
        distance_2points((lat[0] + lat[1]) / 2, lon[0], (lat[0] + lat[1]) / 2, lon[1])
        / 1000
    )
    km_y = distance_2points(lat[0], lon[0], lat[1], lon[0]) / 1000
    return km_x, km_y


def distance_2points(lat1, lon1, lat2, lon2) -> float:
    """Calculate distance between two points in m"""
    return geopy.distance.geodesic((lat1, lon1), (lat2, lon2)).m
