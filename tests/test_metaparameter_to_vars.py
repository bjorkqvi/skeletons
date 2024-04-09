from geo_skeletons import GriddedSkeleton, PointSkeleton
from geo_skeletons.decorators import add_datavar, add_magnitude
import geo_parameters as gp


def test_add_magnitude():
    @add_magnitude(
        name="wind",
        x="u",
        y="v",
        meta=gp.wind.Wind,
        direction="wdir",
    )
    @add_datavar("v", default_value=1, meta=gp.wind.YWind)
    @add_datavar("u", default_value=1, meta=gp.wind.XWind)
    class Magnitude(PointSkeleton):
        pass

    points = Magnitude(x=(0, 1, 2, 4), y=(5, 6, 7, 8))
    assert points.metadata() == {"utm_zone": "33W"}
    points.set_metadata({"general_info": "who knew!?"})
    assert points.metadata() == {"utm_zone": "33W", "general_info": "who knew!?"}
    points.set_u(0)
    assert points.metadata() == {"utm_zone": "33W", "general_info": "who knew!?"}
    assert points.metadata("u") == gp.wind.XWind.meta_dict()
    points.set_metadata({"new": "global"})
    points.set_metadata({"new": "u-specific"}, "u")
    assert points.metadata() == {
        "utm_zone": "33W",
        "general_info": "who knew!?",
        "new": "global",
    }

    assert gp.wind.XWind.meta_dict().items() <= points.metadata("u").items()
    assert points.metadata("u").get("new") == "u-specific"
    breakpoint()
