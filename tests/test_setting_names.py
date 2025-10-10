import xarray as xr
import pytest
from geo_skeletons import PointSkeleton
import os

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


def test_set_name_from_netcdf_taken_from_ds(ds):
    ds.to_netcdf('TestFile.nc')
    points = PointSkeleton.from_netcdf('TestFile.nc')
    assert points.name == 'TestName'
    assert points.ds().name == 'TestName'
    if os.path.exists('TestFile.nc'):
        os.remove('TestFile.nc')

def test_set_name_from_netcdf_no_name_in_ds(ds):
    del ds.attrs['name']
    ds.to_netcdf('TestFile.nc')
    points = PointSkeleton.from_netcdf('TestFile.nc')
    assert points.name == 'Created from TestFile.nc'
    assert points.ds().name == 'Created from TestFile.nc'
    if os.path.exists('TestFile.nc'):
        os.remove('TestFile.nc')

def test_set_new_name_on_creation_netcdf(ds):
    ds.to_netcdf('TestFile.nc')
    points = PointSkeleton.from_netcdf('TestFile.nc', name='NewName')
    assert points.name == 'NewName'
    assert points.ds().name == 'NewName'
    if os.path.exists('TestFile.nc'):
        os.remove('TestFile.nc')