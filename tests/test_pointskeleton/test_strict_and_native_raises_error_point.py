from geo_skeletons import PointSkeleton
import pytest
import numpy as np
def test_get_xy_lonlat():
    with pytest.raises(ValueError):
        PointSkeleton(lon=1, lat=1).x(strict=True, native=True)
    with pytest.raises(ValueError):
        PointSkeleton(lon=1, lat=1).y(strict=True, native=True)
    with pytest.raises(ValueError):
        PointSkeleton(lon=1, lat=1).lon(strict=True, native=True)
    with pytest.raises(ValueError):
        PointSkeleton(lon=1, lat=1).lat(strict=True, native=True)
    with pytest.raises(ValueError):
        PointSkeleton(lon=1, lat=1).xy(strict=True, native=True)
    with pytest.raises(ValueError):
        PointSkeleton(lon=1, lat=1).lonlat(strict=True, native=True)
