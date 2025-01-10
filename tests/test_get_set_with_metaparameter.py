from geo_skeletons import GriddedSkeleton, PointSkeleton
from geo_skeletons.decorators import add_datavar, add_magnitude
import geo_parameters as gp
import numpy as np
import pytest
from geo_skeletons.errors import UnknownVariableError


def test_set_with_metaparameter():
    @add_magnitude(name=gp.wind.Wind("wind2"), x="u", y="v")
    @add_magnitude(name=gp.wind.Wind, x="u", y="v", direction=gp.wind.WindDir)
    @add_datavar(gp.wind.Wind("umag"), default_value=1)
    @add_datavar(gp.wind.YWind("vmodel"), default_value=1)
    @add_datavar(gp.wind.XWind("umodel"), default_value=1)
    @add_datavar(gp.wind.YWind("v"), default_value=1)
    @add_datavar(gp.wind.XWind("u"), default_value=1)
    @add_datavar(gp.wind.XGust("ug"), default_value=1)
    class Magnitude(PointSkeleton):
        pass

    points = Magnitude(x=0, y=0)

    points.set(gp.wind.XGust, 10)
    np.testing.assert_array_almost_equal(points.ug(), 10)
    np.testing.assert_array_almost_equal(points.get(gp.wind.XGust), 10)

    with pytest.raises(UnknownVariableError):
        points.set(gp.wind.XWind, 10)
    with pytest.raises(UnknownVariableError):
        points.get(gp.wind.XWind)

    points.set(gp.wind.XWind("u"), 10)
    np.testing.assert_array_almost_equal(points.u(), 10)
    np.testing.assert_array_almost_equal(points.get(gp.wind.XWind("u")), 10)
    points.set(gp.wind.Wind("ff"), 10)
    np.testing.assert_array_almost_equal(points.ff(), 10)
    np.testing.assert_array_almost_equal(points.get(gp.wind.Wind("ff")), 10)
