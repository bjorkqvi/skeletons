import geopy.distance
import numpy as np
from scipy.spatial.distance import cdist


def get_dist_point(x, y):
    """Calculates the distance to the nearest neaighbour for each point"""
    if len(x) == 1 and len(y) == 1:
        return np.array([0.0])
    points = [(i, j) for i, j in zip(x, y)]
    dist = cdist(points, points, metric="euclidean")
    dist_point = []
    for i in range(dist.shape[0]):
        sl = dist[i,:]
        sl[sl<0.0000001] = 99999999
        dist_point.append(float(np.min(sl)))
    return dist_point

def min_distance(
    lon: float, lat: float, lon_vec: np.ndarray, lat_vec: np.ndarray, npoints: int = 1
) -> tuple[np.ndarray[float], np.ndarray[int]]:
    """Calculates minimum distance [m] between a given point and a list of
    points given in spherical coordinates (lon/lat degrees).

    Also returns index of the found minimum.
    """
    dx = []
    for n, __ in enumerate(lat_vec):
        dx.append(distance_2points(lat, lon, lat_vec[n], lon_vec[n]))
    # inds = np.argpartition(dx, npoints - 1)[:npoints]
    inds = np.argsort(dx)[0:npoints]
    # if npoints > 1:
    #    breakpoint()
    return np.array(dx)[inds], inds

    # return [np.array(dx).min()], [np.array(dx).argmin()]


def min_cartesian_distance(
    x: float, y: float, x_vec: np.ndarray, y_vec: np.ndarray, npoints: int = 1
) -> tuple[np.ndarray[float], np.ndarray[int]]:
    """ "Calculates minimum distance [m] between a given point and list of points given
    in cartesian coordinates [m].

    Also returns incex of found minimum"""
    dx = ((y - y_vec) ** 2 + (x - x_vec) ** 2) ** 0.5
    inds = np.argsort(dx)[0:npoints]
    # if npoints > 1:
    #    breakpoint()
    return np.array(dx)[inds], inds

    # inds = np.argpartition(dx, npoints - 1)[:npoints]
    # # if npoints > 1:
    # #     breakpoint()
    # return dx[inds], inds
    # # return dx.min(), dx.argmin()


def lon_in_km(lat: float, lon: float) -> float:
    """Converts one longitude degree to km for a given latitude and longitude"""
    return distance_2points(lat, lon, lat, lon+1) / 1000


def lat_in_km(lat: float, lon: float) -> float:
    """Converts one latitude degree to km for a given latitude and longitude"""
    return distance_2points(lat, lon, lat + 1, lon) / 1000

def dx_to_dlon(dx: float, lat: float, lon: float) -> float:
    """Converts dx [m] to longitude degrees given a latitude and longitude"""
    one_lon = lon_in_km(lat=lat, lon=lon)*1000 # One latitude degree in metres
    return float(dx/one_lon)

def dy_to_dlat(dy: float, lat: float, lon: float) -> float:
    """Converts dy [m] to latittude degrees given a latitude and longitude"""
    one_lat = lat_in_km(lat=lat, lon=lon)*1000 # One latitude degree in metres
    return float(dy/one_lat)

def dlon_to_dx(dlon: float, lat: float, lon: float) -> float:
    """Converts longitude degrees to dx [m] given a latitude and longitude"""
    one_lon = lon_in_km(lat=lat, lon=lon)*1000 # One latitude degree in metres
    return float(one_lon*dlon)

def dlat_to_dy(dlat: float, lat: float, lon: float) -> float:
    """Converts latitude degrees to dy [m] given a latitude and longitude"""
    one_lat = lat_in_km(lat=lat, lon=lon)*1000 # One latitude degree in metres
    return float(one_lat*dlat)

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
