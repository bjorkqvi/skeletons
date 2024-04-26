from geo_skeletons import GriddedSkeleton, PointSkeleton
from geo_skeletons.decorators import add_datavar, add_magnitude
import geo_parameters as gp
import numpy as np


def test_cf_no_info():
    @add_magnitude(name="wind", x="u", y="v", direction="wdir", dir_type="from")
    @add_datavar("v", default_value=1)
    @add_datavar("u", default_value=1)
    class Magnitude(PointSkeleton):
        pass

    points = Magnitude(x=0, y=0)

    assert points.find_cf(gp.wind.XWind.standard_name()) == []
    assert points.find_cf(gp.wind.YWind.standard_name()) == []
    assert points.find_cf(gp.wind.Wind.standard_name()) == []
    assert points.find_cf(gp.wind.WindDir.standard_name()) == []


def test_cf():
    @add_magnitude(name=gp.wind.Wind, x="u", y="v", direction=gp.wind.WindDir)
    @add_datavar(gp.wind.YWind("v"), default_value=1)
    @add_datavar(gp.wind.XWind("u"), default_value=1)
    class Magnitude(PointSkeleton):
        pass

    points = Magnitude(x=0, y=0)
    assert points.find_cf(gp.wind.XWind.standard_name()) == ["u"]
    assert points.find_cf(gp.wind.YWind.standard_name()) == ["v"]
    assert points.find_cf(gp.wind.Wind.standard_name()) == ["wind"]
    assert points.find_cf(gp.wind.WindDir.standard_name()) == ["wind_dir"]


def test_cf_several():
    @add_magnitude(name=gp.wind.Wind("wind2"), x="u", y="v")
    @add_magnitude(name=gp.wind.Wind, x="u", y="v", direction=gp.wind.WindDir)
    @add_datavar(gp.wind.Wind("umag"), default_value=1)
    @add_datavar(gp.wind.YWind("vmodel"), default_value=1)
    @add_datavar(gp.wind.XWind("umodel"), default_value=1)
    @add_datavar(gp.wind.YWind("v"), default_value=1)
    @add_datavar(gp.wind.XWind("u"), default_value=1)
    class Magnitude(PointSkeleton):
        pass

    points = Magnitude(x=0, y=0)

    assert points.find_cf(gp.wind.XWind.standard_name()) == ["u", "umodel"]
    assert points.find_cf(gp.wind.YWind.standard_name()) == ["v", "vmodel"]
    assert points.find_cf(gp.wind.Wind.standard_name()) == ["umag", "wind", "wind2"]
    assert points.find_cf(gp.wind.WindDir.standard_name()) == ["wind_dir"]
