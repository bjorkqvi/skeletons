from geo_skeletons import GriddedSkeleton, PointSkeleton
from geo_skeletons.decorators import add_datavar, add_magnitude, add_mask
import geo_parameters as gp


def test_add_datavar():
    @add_datavar(gp.wind.XWind("u"), default_value=1)
    @add_datavar(gp.wind.YWind("v"), default_value=1)
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


def test_add_magnitude():
    @add_magnitude(
        name=gp.wind.Wind("wnd"),
        x="u",
        y="v",
        direction=gp.wind.WindDir,
    )
    @add_datavar(gp.wind.XWind("u"), default_value=1)
    @add_datavar(gp.wind.YWind("v"), default_value=1)
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
    assert points.magnitudes() == ["wnd"]
    assert points.directions() == [gp.wind.WindDir.short_name()]


def test_add_mask():
    @add_mask(name=gp.wave.Hs("land"), default_value=0, opposite_name="sea")
    @add_datavar(gp.wind.XWind("u"), default_value=1)
    @add_datavar(gp.wind.YWind("v"), default_value=1)
    class Magnitude(PointSkeleton):
        pass

    points = Magnitude(x=(0, 1, 2, 4), y=(5, 6, 7, 8))
    points.set_land_mask(0)
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
