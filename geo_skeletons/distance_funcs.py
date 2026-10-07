from functools import lru_cache

import geopy.distance
import numpy as np
from scipy.spatial import cKDTree

@lru_cache(maxsize=10)
def get_dist_point(x: np.ndarray, y: np.ndarray):
    """Calculates the distance to the nearest neighbor for each point.
    Used in case we have very high or low latitudes so that a cartesian UTM projection is not available"""
    if len(x) == 1 and len(y) == 1:
        return np.array([0.0])
    
    # Combine x and y into a single array of points
    points = np.column_stack((x, y))
    
    # Use KDTree for efficient nearest neighbor queries
    tree = cKDTree(points)
    
    # Query the tree for the second nearest neighbor (k=2)
    distances, _ = tree.query(points, k=2)
    
    # Return the distances to the nearest neighbor (excluding self)
    return distances[:, 1]



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

    inds = np.argsort(dx)[0:npoints]

    return np.array(dx)[inds], inds




def min_cartesian_distance(
    x: float, y: float, x_vec: np.ndarray, y_vec: np.ndarray, npoints: int = 1
) -> tuple[np.ndarray[float], np.ndarray[int]]:
    """ "Calculates minimum distance [m] between a given point and list of points given
    in cartesian coordinates [m].

    Also returns incex of found minimum"""
    dx = ((y - y_vec) ** 2 + (x - x_vec) ** 2) ** 0.5
    inds = np.argsort(dx)[0:npoints]
    return np.array(dx)[inds], inds



def lon_in_km(lat: float, lon: float) -> float:
    """Converts one longitude degree to km for a given latitude and longitude"""
    return distance_2points(lat, lon, lat, lon+1) / 1000


def lat_in_km(lat: float, lon: float) -> float:
    """Converts one latitude degree to km for a given latitude and longitude"""
    if lat +1 < 90:
        return distance_2points(lat, lon, lat + 1, lon) / 1000
    return distance_2points(lat-1, lon, lat, lon) / 1000

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


def distance_2points(lat1, lon1, lat2, lon2, geodesic: bool=True) -> float:
    """Calculate distance between two points in m"""
    if geodesic:
        return geopy.distance.geodesic((lat1, lon1), (lat2, lon2)).m
    else:
        dx = lon_in_km((lat2+lat1)/2, (lon2+lon1)/2)*1000*(lon2-lon1)
        dy = lat_in_km((lat2+lat1)/2, (lon2+lon1)/2)*1000*(lat2-lat1)
        return (dx**2+dy**2)**0.5
