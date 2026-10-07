from geo_skeletons import GriddedSkeleton
import pytest
import numpy as np
def test_get_xy_lonlat():
    with pytest.raises(ValueError):
        GriddedSkeleton(lon=1, lat=1).x(strict=True, native=True)
    with pytest.raises(ValueError):
        GriddedSkeleton(lon=1, lat=1).y(strict=True, native=True)
    with pytest.raises(ValueError):
        GriddedSkeleton(lon=1, lat=1).lon(strict=True, native=True)
    with pytest.raises(ValueError):
        GriddedSkeleton(lon=1, lat=1).lat(strict=True, native=True)
    with pytest.raises(ValueError):
        GriddedSkeleton(lon=1, lat=1).xy(strict=True, native=True)
    with pytest.raises(ValueError):
        GriddedSkeleton(lon=1, lat=1).lonlat(strict=True, native=True)

def test_get_dxy_lonlat():
    with pytest.raises(ValueError):
        GriddedSkeleton(lon=1, lat=1).dx(strict=True, native=True)
    with pytest.raises(ValueError):
        GriddedSkeleton(lon=1, lat=1).dy(strict=True, native=True)
    with pytest.raises(ValueError):
        GriddedSkeleton(lon=1, lat=1).dlon(strict=True, native=True)
    with pytest.raises(ValueError):
        GriddedSkeleton(lon=1, lat=1).dlat(strict=True, native=True)
    with pytest.raises(ValueError):
        GriddedSkeleton(lon=1, lat=1).dmx(strict=True, native=True)
    with pytest.raises(ValueError):
        GriddedSkeleton(lon=1, lat=1).dmy(strict=True, native=True)
