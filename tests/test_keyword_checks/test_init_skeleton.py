from geo_skeletons import PointSkeleton, GriddedSkeleton

import pytest

def test_point():
    data = PointSkeleton(x=2, y=6)
    with pytest.raises(TypeError):
        data = PointSkeleton(x=2, y=6, c=5)
    data = PointSkeleton(x=2, y=6, crs=(33,'W'))
    with pytest.raises(TypeError):
        data = PointSkeleton(x=2, y=6, c=5, crs=(33,'W'))

    data = PointSkeleton(lon=2, lat=6)
    with pytest.raises(TypeError):
        data = PointSkeleton(lon=2, lat=6, c=5)
    data = PointSkeleton(lon=2, lat=6, crs=(33,'W'))
    with pytest.raises(TypeError):
        data = PointSkeleton(lon=2, lat=6, c=5, crs=(33,'W'))

def test_point_with_time():
    data = PointSkeleton.add_time()(x=2, y=6, time=('2020-01-01', '2020-01-02'))
    with pytest.raises(TypeError):
        data = PointSkeleton.add_time()(x=2, y=6, time=('2020-01-01', '2020-01-02'), c=5)
    data = PointSkeleton.add_time()(x=2, y=6, crs=(33,'W'), time=('2020-01-01', '2020-01-02'))
    with pytest.raises(TypeError):
        data = PointSkeleton.add_time()(x=2, y=6, c=5, crs=(33,'W'), time=('2020-01-01', '2020-01-02'))

    data = PointSkeleton.add_time()(lon=2, lat=6, time=('2020-01-01', '2020-01-02'))
    with pytest.raises(TypeError):
        data = PointSkeleton.add_time()(lon=2, lat=6, time=('2020-01-01', '2020-01-02'), c=5, d=70)
    data = PointSkeleton.add_time()(lon=2, lat=6, crs=(33,'W'), time=('2020-01-01', '2020-01-02'))
    with pytest.raises(TypeError):
        data = PointSkeleton.add_time()(lon=2, lat=6, c=5, crs=(33,'W'), time=('2020-01-01', '2020-01-02'))

def test_point_with_coord():
    data = PointSkeleton.add_coord('c')(x=2, y=6,  c=5)
    with pytest.raises(TypeError):
        data = PointSkeleton.add_coord('c')(x=2, y=6,  c=5, d=6)
    data = PointSkeleton.add_coord('c')(x=2, y=6, c=5,crs=(33,'W'),)
    with pytest.raises(TypeError):
        data = PointSkeleton.add_coord('c')(x=2, y=6, c=5, d=6, crs=(33,'W'))

    data = PointSkeleton.add_coord('c')(lon=2, lat=6, c=5)
    with pytest.raises(TypeError):
        data = PointSkeleton.add_coord('c')(lon=2, lat=6, time=('2020-01-01', '2020-01-02'), c=5)
    data = PointSkeleton.add_coord('c')(lon=2, lat=6, crs=(33,'W'), c=6)
    with pytest.raises(TypeError):
        data = PointSkeleton.add_coord('c')(lon=2, lat=6, c=5, crs=(33,'W'), time=('2020-01-01', '2020-01-02'))


def test_grid():
    data = GriddedSkeleton(x=2, y=6)
    with pytest.raises(TypeError):
        data = GriddedSkeleton(x=2, y=6, c=5)
    data = GriddedSkeleton(x=2, y=6, crs=(33,'W'))
    with pytest.raises(TypeError):
        data = GriddedSkeleton(x=2, y=6, c=5, crs=(33,'W'))

    data = GriddedSkeleton(lon=2, lat=6)
    with pytest.raises(TypeError):
        data = GriddedSkeleton(lon=2, lat=6, c=5)
    data = GriddedSkeleton(lon=2, lat=6, crs=(33,'W'))
    with pytest.raises(TypeError):
        data = GriddedSkeleton(lon=2, lat=6, c=5, crs=(33,'W'))

def test_grid_with_time():
    data = GriddedSkeleton.add_time()(x=2, y=6, time=('2020-01-01', '2020-01-02'))
    with pytest.raises(TypeError):
        data = GriddedSkeleton.add_time()(x=2, y=6, time=('2020-01-01', '2020-01-02'), c=5)
    data = GriddedSkeleton.add_time()(x=2, y=6, crs=(33,'W'), time=('2020-01-01', '2020-01-02'))
    with pytest.raises(TypeError):
        data = GriddedSkeleton.add_time()(x=2, y=6, c=5, crs=(33,'W'), time=('2020-01-01', '2020-01-02'))

    data = GriddedSkeleton.add_time()(lon=2, lat=6, time=('2020-01-01', '2020-01-02'))
    with pytest.raises(TypeError):
        data = GriddedSkeleton.add_time()(lon=2, lat=6, time=('2020-01-01', '2020-01-02'), c=5, d=70)
    data = GriddedSkeleton.add_time()(lon=2, lat=6, crs=(33,'W'), time=('2020-01-01', '2020-01-02'))
    with pytest.raises(TypeError):
        data = GriddedSkeleton.add_time()(lon=2, lat=6, c=5, crs=(33,'W'), time=('2020-01-01', '2020-01-02'))

def test_grid_with_coord():
    data = GriddedSkeleton.add_coord('c')(x=2, y=6,  c=5)
    with pytest.raises(TypeError):
        data = GriddedSkeleton.add_coord('c')(x=2, y=6,  c=5, d=6)
    data = GriddedSkeleton.add_coord('c')(x=2, y=6, c=5,crs=(33,'W'),)
    with pytest.raises(TypeError):
        data = GriddedSkeleton.add_coord('c')(x=2, y=6, c=5, d=6, crs=(33,'W'))

    data = GriddedSkeleton.add_coord('c')(lon=2, lat=6, c=5)
    with pytest.raises(TypeError):
        data = GriddedSkeleton.add_coord('c')(lon=2, lat=6, time=('2020-01-01', '2020-01-02'), c=5)
    data = GriddedSkeleton.add_coord('c')(lon=2, lat=6, crs=(33,'W'), c=6)
    with pytest.raises(TypeError):
        data = GriddedSkeleton.add_coord('c')(lon=2, lat=6, c=5, crs=(33,'W'), time=('2020-01-01', '2020-01-02'))