from geo_skeletons import GriddedSkeleton, PointSkeleton
from geo_skeletons.decorators import add_datavar, add_magnitude
import numpy as np
import pytest
import geo_parameters as gp


def test_angular_str():
    @add_datavar("stokes_dir", default_value=0, direction_from=False)
    @add_datavar("stokes", default_value=0.1)
    @add_magnitude(name="wind", x="u", y="v", direction="wdir")
    @add_datavar("v", default_value=-1)
    @add_datavar("u", default_value=1)
    class Magnitude(PointSkeleton):
        pass

    points = Magnitude(x=(0, 1, 2), y=(5, 6, 7))
    points.deactivate_dask()

    assert points.u() is None
    assert points.v() is None
    assert points.wind() is None
    assert points.wdir() is None
    assert points.stokes() is None
    assert points.stokes_dir() is None

    np.testing.assert_almost_equal(np.median(points.wdir(empty=True)), 135 + 180)
    np.testing.assert_almost_equal(
        np.median(points.wdir(empty=True, angular=True)), -np.pi / 4
    )

    np.testing.assert_almost_equal(np.median(points.stokes_dir(empty=True)), 0)
    np.testing.assert_almost_equal(
        np.median(points.stokes_dir(empty=True, angular=True)), np.pi / 2
    )
    with pytest.raises(ValueError):
        points.stokes(angular=True)


def test_angular_gp():
    @add_datavar(gp.wave.StokesDir, default_value=0)
    @add_datavar(gp.wave.Stokes, default_value=0.1)
    @add_magnitude(gp.wind.Wind, x="u", y="v", direction=gp.wind.WindDir("wdir"))
    @add_datavar(gp.wind.YWind("v"), default_value=-1)
    @add_datavar(gp.wind.YWind("u"), default_value=1)
    class Magnitude(PointSkeleton):
        pass

    points = Magnitude(x=(0, 1, 2), y=(5, 6, 7))
    points.deactivate_dask()

    assert points.u() is None
    assert points.v() is None
    assert points.wind() is None
    assert points.wdir() is None
    assert points.us() is None
    assert points.us_dir() is None

    np.testing.assert_almost_equal(np.median(points.wdir(empty=True)), 135 + 180)
    np.testing.assert_almost_equal(
        np.median(points.wdir(empty=True, angular=True)), -np.pi / 4
    )

    np.testing.assert_almost_equal(np.median(points.us_dir(empty=True)), 0)
    np.testing.assert_almost_equal(
        np.median(points.us_dir(empty=True, angular=True)), np.pi / 2
    )
    with pytest.raises(ValueError):
        points.us(angular=True)


def test_angular_gp_flip_dir():
    @add_datavar(gp.wave.StokesDirFrom, default_value=0)
    @add_datavar(gp.wave.Stokes, default_value=0.1)
    @add_magnitude(gp.wind.Wind, x="u", y="v", direction=gp.wind.WindDirTo("wdir"))
    @add_datavar(gp.wind.YWind("v"), default_value=-1)
    @add_datavar(gp.wind.YWind("u"), default_value=1)
    class Magnitude(PointSkeleton):
        pass

    points = Magnitude(x=(0, 1, 2), y=(5, 6, 7))
    points.deactivate_dask()

    assert points.u() is None
    assert points.v() is None
    assert points.wind() is None
    assert points.wdir() is None
    assert points.us() is None
    assert points.us_dir() is None

    np.testing.assert_almost_equal(np.median(points.wdir(empty=True)), 135)
    np.testing.assert_almost_equal(
        np.median(points.wdir(empty=True, angular=True)), -np.pi / 4
    )

    np.testing.assert_almost_equal(np.median(points.us_dir(empty=True)), 0)
    np.testing.assert_almost_equal(
        np.median(points.us_dir(empty=True, angular=True)), -np.pi / 2 + 2 * np.pi
    )
    with pytest.raises(ValueError):
        points.us(angular=True)
