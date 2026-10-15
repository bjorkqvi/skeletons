from functools import lru_cache
from .errors import SkeletonError, ProjectionError
from .managers import proj_manager
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


def determine_nx(data, x_type: str, nx: int, dx: float, dmx: float, dlon: float, dnmi: float, floating_edge: bool) -> tuple[int, float]:
    """Determines how many points is needed to get the desired resolution in one dimension
    x_type is 'x' or 'y', determining if we are in x/lon or y/lat direction.
    
    nx: number of points
    dx: cartesian grid spacing in metres (either dx or dy depending on x_type)
    dlon: spherical grid spacing in degrees (either dlon or dlat depending on x_type)
    dnmi: spacing in nautical miles

    floating_edge  [bool]: If True, then spacing is exact and edge of grid is moved.
    
    return nx and right/east (or upper/north) edge of grid"""

    if all(v is None for v in [nx, dx, dmx, dlon, dnmi]):
        raise ValueError("At least one of nx, dx, dmx, dlon, or dnmi must be specified.")

    assert x_type in ['x', 'y']
    
    if x_type == "x":
        lon_type = "lon"
    else:
        lon_type = "lat"

    x_end = data.edges(x_type, native=True)[1]

    if nx:
        return int(nx), x_end

    if dnmi:
        if data.core.is_cartesian():
            dmx = dnmi * 1850.0
        elif not data.core.is_projected():
            dlon = dnmi / 60.0
            if x_type == 'x':
                lon = sum(data.edges('lon'))/2
                lat = sum(data.edges('lat'))/2
                dy = lat_in_km(lat=lat, lon=lon)*1000*dlon
                dlon = dx_to_dlon(dy, lat=lat, lon=lon)
        else: # Rotated degrees
            dlon = dnmi / 60.0
            if x_type == 'x':
                x = data.edges('x')
                y = data.edges('y')
                #points = PointSkeleton(x=x, y=y, crs=data.proj.crs())
                #lon, lat = points.lonlat()
                lon, lat = proj_manager._lonlat(x=x, y=y, crs=data.proj.crs())
                lon, lat = sum(lon)/2, sum(lat)/2
                dy = lat_in_km(lat=lat, lon=lon)*1000*dlon
                dlon = dx_to_dlon(dy, lat=lat, lon=lon)
    
    # Convert dx/dlon to the native spacing for the grid
    if data.core.is_cartesian():
        if dmx:
                spacing = dmx
        elif dx:
            spacing = dx
        else:
            if floating_edge:
                raise SkeletonError(
                    "Grid is projected, so cant set exact dlon/dlat using floating_edge!"
                )
            
            # Reproject edges to get mid point of grid in spherical coordinates
            x = data.edges('x')
            y = data.edges('y')
            #points = PointSkeleton(x=x, y=y, crs=data.proj.crs())
            if data.proj.is_engineering():
                raise SkeletonError("Can't set spherical spacing (dlon/dlat) since no projeection is defined!")
            lon, lat = proj_manager._lonlat(x=x, y=y, crs=data.proj.crs())

            lon, lat = sum(lon)/2, sum(lat)/2
            if x_type == 'x':
                spacing = dlon_to_dx(dlon, lat=lat, lon=lon)
            else:
                spacing = dlat_to_dy(dlon, lat=lat, lon=lon)
            
    elif data.core.is_rotated(): # Rotated lon/lat
        if dx:
            spacing = dx
        if dlon:

            x = data.edges('x')
            y = data.edges('y')
            #points = PointSkeleton(x=x, y=y, crs=data.proj.crs())
            #lon, lat = points.lonlat()
            lon, lat = proj_manager._lonlat(x=x, y=y, crs=data.proj.crs())
            lon, lat = sum(lon)/2, sum(lat)/2
            if x_type == 'x':
                dmx = dlon_to_dx(dlon, lat=lat, lon=lon)
            else:
                dmx = dlat_to_dy(dlon, lat=lat, lon=lon)
        if dmx:

            if x_type == 'x':
                lon = data.isel(y=0).edges('lon')
                lat = data.isel(y=0).edges('lat')
                d1 = distance_2points(lat[0],lon[0], lat[1], lon[1], geodesic=False)
                n1 = d1/dmx + 1

                lon = data.isel(y=-1).edges('lon')
                lat = data.isel(y=-1).edges('lat')
                d2 = distance_2points(lat[0],lon[0], lat[1], lon[1], geodesic=False)
                n2 = d2/dmx + 1
                n = int(np.round((n1+n2)/2))
                spacing = (data.edges('x')[1] - data.edges('x')[0])/(n-1)
                
            else:
                lon = data.isel(x=0).edges('lon')
                lat = data.isel(x=0).edges('lat')
                d1 = distance_2points(lat[0],lon[0], lat[1], lon[1], geodesic=False)
                n1 = d1/dmx + 1

                lon = data.isel(x=-1).edges('lon')
                lat = data.isel(x=-1).edges('lat')
                d2 = distance_2points(lat[0],lon[0], lat[1], lon[1], geodesic=False)
                n2 = d2/dmx + 1

                n = int(np.round((n1+n2)/2))
                spacing = (data.edges('y')[1] - data.edges('y')[0])/(n-1)

    elif not data.core.is_projected():
        if dlon:
            spacing = dlon
        elif dx:
            if data.core.is_rotated():
                raise SkeletonError(f"Can't use spacing in d{x_type} for grids spherical grids with a projection with units in rotated degrees! Use dm{x_type}, n{x_type} or dlon/dlat instead.")                        
            else:
                dmx = dx
        
        if dmx:                    
            if floating_edge:
                raise SkeletonError(
                    "Grid is spherical, so cant set exact dx/dy using floating_edge!"
                )
            
            lon = data.edges('lon')
            lat = data.edges('lat')
            lon, lat = sum(lon)/2, sum(lat)/2
            if x_type == 'x':
                spacing = dx_to_dlon(dmx, lat=lat, lon=lon)
            else:
                spacing = dy_to_dlat(dmx, lat=lat, lon=lon)


        
    nx = (
        np.round((data.edges(lon_type, native=True)[1] - data.edges(lon_type, native=True)[0]) / spacing)
        + 1
    )

    if floating_edge:
        x_end = data.edges(x_type, native=True)[0] + (nx - 1) * spacing

    return nx.astype(int), x_end