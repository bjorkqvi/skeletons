from geo_skeletons.point_skeleton import PointSkeleton
from geo_skeletons.decorators import add_coord, add_datavar, add_mask

from geo_skeletons.errors import VariableExistsError
import pytest


def test_two_vars():
    with pytest.raises(VariableExistsError):

        @add_datavar("u")
        @add_datavar("u")
        class Wrong(PointSkeleton):
            pass

    @add_datavar("u")
    class Wrong(PointSkeleton):
        pass

    points = Wrong(x=0, y=0)
    points.add_datavar("v")
    with pytest.raises(VariableExistsError):
        points.add_datavar("u")


def test_two_coords():
    with pytest.raises(VariableExistsError):

        @add_coord(name="u")
        @add_coord("u")
        class Wrong(PointSkeleton):
            pass


def test_mix():
    @add_mask("sea")
    @add_datavar("v")
    @add_datavar("u")
    @add_coord("z")
    class Wrong(PointSkeleton):
        pass

    points = Wrong(x=0, y=0, z=0)
    with pytest.raises(VariableExistsError):
        points.add_magnitude(name="u", x="u", y="v", dir_type="from")

    with pytest.raises(VariableExistsError):
        points.add_magnitude(name="umag", x="u", y="v", direction="v", dir_type="from")

    with pytest.raises(VariableExistsError):
        points.add_magnitude(name="umag", x="u", y="v", direction="v", dir_type="from")

    with pytest.raises(VariableExistsError):

        @add_mask(name="sea", default_value=0)
        class Wrong2(Wrong):
            pass

    points.add_datavar("sea")
