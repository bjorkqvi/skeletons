from geo_skeletons import PointSkeleton, GriddedSkeleton
from geo_skeletons.decorators import add_datavar, add_magnitude, dynamic
import geo_parameters as gp
import pytest
from geo_skeletons.errors import StaticSkeletonError


def test_add_decorator_to_static():
    assert PointSkeleton.core.static

    @add_magnitude(gp.wind.Wind("mag"), x="u", y="v", direction=gp.wind.WindDir("dir"))
    @add_datavar(name=gp.wind.YWind("v"), default_value=0.0)
    @add_datavar(name=gp.wind.XWind("u"), default_value=0.0)
    class Dummy(PointSkeleton):
        pass

    assert PointSkeleton.core.static

    assert Dummy.core.static

    @add_magnitude(gp.wind.Wind("mag"), x="u", y="v", direction=gp.wind.WindDir("dir"))
    @add_datavar(name=gp.wind.YWind("v"), default_value=0.0)
    @add_datavar(name=gp.wind.XWind("u"), default_value=0.0)
    class GriddedDummy(GriddedSkeleton):
        pass

    assert GriddedDummy.core.static


def test_add_dynamic_to_static():
    class Dummy(PointSkeleton):
        pass

    assert Dummy.core.static
    points = Dummy(lon=0, lat=0)

    assert points.core.static
    assert Dummy.core.static
    with pytest.raises(StaticSkeletonError):
        points.add_datavar("hs")

    class GriddedDummy(GriddedSkeleton):
        pass

    assert GriddedDummy.core.static
    grid = GriddedDummy(lon=0, lat=0)
    assert grid.core.static
    assert GriddedDummy.core.static
    with pytest.raises(StaticSkeletonError):
        grid.add_datavar("hs")


def test_add_dynamic_magnitude_to_static():
    @add_datavar(name=gp.wind.YWind("v"), default_value=0.0)
    @add_datavar(name=gp.wind.XWind("u"), default_value=0.0)
    class Dummy(PointSkeleton):
        pass

    assert Dummy.core.static
    points = Dummy(lon=0, lat=0)
    assert points.core.static
    assert Dummy.core.static
    with pytest.raises(StaticSkeletonError):
        points.add_datavar("hs")

    with pytest.raises(StaticSkeletonError):
        points.add_magnitude(
            "wind", x="u", y="v", direction="wind_dir", dir_type="from"
        )

    @add_datavar(name=gp.wind.YWind("v"), default_value=0.0)
    @add_datavar(name=gp.wind.XWind("u"), default_value=0.0)
    class GriddedDummy(GriddedSkeleton):
        pass

    assert GriddedDummy.core.static
    grid = GriddedDummy(lon=0, lat=0)

    assert grid.core.static
    assert GriddedDummy.core.static
    with pytest.raises(StaticSkeletonError):
        grid.add_datavar("hs")

    with pytest.raises(StaticSkeletonError):
        grid.add_magnitude("wind", x="u", y="v", direction="wind_dir", dir_type="from")


def test_init_static_class_as_dynamic():
    @add_datavar(name=gp.wind.YWind("v"), default_value=0.0)
    @add_datavar(name=gp.wind.XWind("u"), default_value=0.0)
    class Dummy(PointSkeleton):
        pass

    assert Dummy.core.static

    points = Dummy(lon=0, lat=0)
    points.core.static = False
    assert not points.core.static
    assert Dummy.core.static
    points.add_datavar("hs")
    points.add_magnitude("wind", x="u", y="v", direction="wind_dir", dir_type="from")

    @add_datavar(name=gp.wind.YWind("v"), default_value=0.0)
    @add_datavar(name=gp.wind.XWind("u"), default_value=0.0)
    class GriddedDummy(GriddedSkeleton):
        pass

    assert GriddedDummy.core.static

    grid = GriddedDummy(lon=0, lat=0)
    grid.core.static = False
    assert not grid.core.static
    assert GriddedDummy.core.static
    grid.add_datavar("hs")
    grid.add_magnitude("wind", x="u", y="v", direction="wind_dir", dir_type="from")


def test_create_dynamic_class():
    @dynamic
    @add_datavar(name=gp.wind.YWind("v"), default_value=0.0)
    @add_datavar(name=gp.wind.XWind("u"), default_value=0.0)
    class Dummy(PointSkeleton):
        pass

    assert not Dummy.core.static

    points = Dummy(lon=0, lat=0)
    assert not points.core.static
    assert not Dummy.core.static
    points.add_datavar("hs")
    points.add_magnitude("wind", x="u", y="v", direction="wind_dir", dir_type="from")

    @dynamic
    @add_datavar(name=gp.wind.YWind("v"), default_value=0.0)
    @add_datavar(name=gp.wind.XWind("u"), default_value=0.0)
    class GriddedDummy(GriddedSkeleton):
        pass

    assert not GriddedDummy.core.static

    grid = GriddedDummy(lon=0, lat=0)
    assert not grid.core.static
    assert not GriddedDummy.core.static
    grid.add_datavar("hs")
    grid.add_magnitude("wind", x="u", y="v", direction="wind_dir", dir_type="from")


def test_create_dynamic_class_init_static():
    @dynamic
    @add_datavar(name=gp.wind.YWind("v"), default_value=0.0)
    @add_datavar(name=gp.wind.XWind("u"), default_value=0.0)
    class Dummy(PointSkeleton):
        pass

    assert not Dummy.core.static
    points = Dummy(lon=0, lat=0)
    points.core.static = True
    assert points.core.static
    assert not Dummy.core.static

    with pytest.raises(StaticSkeletonError):
        points.add_datavar("hs")

    with pytest.raises(StaticSkeletonError):
        points.add_magnitude(
            "wind", x="u", y="v", direction="wind_dir", dir_type="from"
        )

    @dynamic
    @add_datavar(name=gp.wind.YWind("v"), default_value=0.0)
    @add_datavar(name=gp.wind.XWind("u"), default_value=0.0)
    class GriddedDummy(GriddedSkeleton):
        pass

    assert not GriddedDummy.core.static
    grid = GriddedDummy(lon=0, lat=0)
    grid.core.static = True
    assert not GriddedDummy.core.static
    assert grid.core.static
    with pytest.raises(StaticSkeletonError):
        grid.add_datavar("hs")

    with pytest.raises(StaticSkeletonError):
        grid.add_magnitude("wind", x="u", y="v", direction="wind_dir", dir_type="from")
