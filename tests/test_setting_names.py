import xarray as xr
import pytest
from geo_skeletons import PointSkeleton

@pytest.fixture
def ds():
    longitude = [0, 1, 2]
    latitude = [10, 20, 30]

    ds = xr.Dataset(
        {
            "lon": ("lon", longitude),
            "lat": ("lat", latitude),
        }
    )

    ds.attrs["name"] = "TestName"
    return ds

def test_set_name_from_ds(ds):
    points = PointSkeleton.from_ds(ds)
    assert points.name == 'TestName'
    assert points.ds().name == 'TestName'

def test_set_new_name_on_creation(ds):
    points = PointSkeleton.from_ds(ds, name = 'NewName')
    assert points.name == 'NewName'
    assert points.ds().name == 'NewName'