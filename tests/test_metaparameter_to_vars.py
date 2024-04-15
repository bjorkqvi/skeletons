from geo_skeletons import GriddedSkeleton, PointSkeleton
from geo_skeletons.decorators import (
    add_datavar,
    add_magnitude,
    add_mask,
    add_frequency,
    add_time,
    add_direction,
)
import geo_parameters as gp

import pandas as pd


def test_lonlat():
    points = PointSkeleton(lon=[1, 2], lat=[4, 5])
    assert points.metadata("lon") == {
        "short_name": "lon",
        "long_name": "longitude",
        "standard_name": "longitude",
        "unit": "degrees_east",
    }
    assert points.metadata("lat") == {
        "short_name": "lat",
        "long_name": "latitude",
        "standard_name": "latitude",
        "unit": "degrees_north",
    }

    assert points.metadata("inds") == {
        "short_name": "inds",
        "long_name": "index_of_points",
        "standard_name": "index_of_geophysical_points",
        "unit": "-",
    }


def test_xy():
    points = PointSkeleton(x=[1, 2], y=[4, 5])
    assert points.metadata("x") == {
        "short_name": "x",
        "long_name": "x_distance",
        "standard_name": "distance_in_x_direction",
        "unit": "m",
    }
    assert points.metadata("y") == {
        "short_name": "y",
        "long_name": "y_distance",
        "standard_name": "distance_in_y_direction",
        "unit": "m",
    }

    assert points.metadata("inds") == {
        "short_name": "inds",
        "long_name": "index_of_points",
        "standard_name": "index_of_geophysical_points",
        "unit": "-",
    }


def test_lonlat_gridded():
    points = GriddedSkeleton(lon=[1, 2], lat=[4, 5])
    assert points.metadata("lon") == {
        "short_name": "lon",
        "long_name": "longitude",
        "standard_name": "longitude",
        "unit": "degrees_east",
    }
    assert points.metadata("lat") == {
        "short_name": "lat",
        "long_name": "latitude",
        "standard_name": "latitude",
        "unit": "degrees_north",
    }
    assert points.metadata("inds") == {}


def test_freq_dir_time():
    @add_time()
    @add_direction()
    @add_frequency()
    class Expanded(PointSkeleton):
        pass

    points = Expanded(
        lon=[1, 2],
        lat=[4, 5],
        freq=range(10),
        dirs=range(360),
        time=pd.date_range("2020-01-01 00:00", "2020-01-01 23:00", freq="h"),
    )
    assert points.metadata("freq") == {
        "short_name": "freq",
        "long_name": "frequency",
        "standard_name": "wave_frequency",
        "unit": "1/s",
    }

    assert points.metadata("dirs") == {
        "short_name": "dirs",
        "long_name": "wave_direction",
        "standard_name": "sea_surface_wave_from_direction",
        "unit": "deg",
    }
    assert points.metadata("time") == {}


def test_dirto():

    @add_direction(direction_from=False)
    class Expanded(PointSkeleton):
        pass

    points = Expanded(lon=[1, 2], lat=[4, 5], dirs=range(360))

    assert points.metadata("dirs") == {
        "short_name": "dirs",
        "long_name": "wave_direction",
        "standard_name": "sea_surface_wave_to_direction",
        "unit": "deg",
    }


def test_add_datavar():
    @add_datavar(gp.wind.XWind("u"), default_value=1)
    @add_datavar(gp.wind.YWind("v"), default_value=1)
    class Magnitude(PointSkeleton):
        pass

    points = Magnitude(x=(0, 1, 2, 4), y=(5, 6, 7, 8))

    assert points.metadata() == {}
    points.set_utm((33, "W"))
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

    assert points.metadata() == {}
    points.set_utm((33, "W"))
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
    assert points.directions() == [gp.wind.WindDir.name]


def test_add_mask():
    @add_mask(name=gp.wave.Hs("land"), default_value=0, opposite_name="sea")
    @add_datavar(gp.wind.XWind("u"), default_value=1)
    @add_datavar(gp.wind.YWind("v"), default_value=1)
    class Magnitude(PointSkeleton):
        pass

    points = Magnitude(x=(0, 1, 2, 4), y=(5, 6, 7, 8))
    points.set_land_mask(0)
    assert points.metadata() == {}
    points.set_utm((33, "W"))
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
