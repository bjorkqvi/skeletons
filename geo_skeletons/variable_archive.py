"""This module takes care of storing information about known coordinates and aliases"""

import geo_parameters as gp
from typing import Union
from geo_parameters.metaparameter import MetaParameter

# These are used e.g. by the coordinate_manager to keep track of mandaroty coordinates and added coordinates
SPATIAL_COORDS = ["y", "x", "lat", "lon", "inds"]


# List assumed coordinate aliases here. These are used e.g. by decoders.
COORD_ALIASES = {
    "time": ["time"],
    gp.grid.X: ["x"],
    gp.grid.Y: ["y"],
    gp.grid.Lon: ["lon", "longitude"],
    gp.grid.Lat: ["lat", "latitude"],
    gp.wave.Freq: ["freq", "frequency"],
    gp.wave.Dirs: ["dirs", "directions", "direction", "theta"],
}

LIST_OF_COORD_ALIASES = [v for v in COORD_ALIASES.values()]

# List assumed data variable aliases here
VAR_ALIASES = {
    gp.wave.Hs: ["hs", "hsig", "h13", "swh", "hm0", "vhm0"],
    gp.wave.Tp: ["tp"],
    gp.wave.Fp: ["fp"],
    gp.wave.Tm01: ["tm01", "t01", "tm1"],
    gp.wave.Tm02: ["tm02", "t02", "tm2"],
    gp.wave.Tm_10: ["tm_10"],
    gp.wave.Tz: ["tz"],
    gp.wind.Wind: ["ff", "wind", "wind_speed", "windspeed"],
    gp.wind.XWind: ["x_wind", "xwnd"],
    gp.wind.YWind: ["y_wind", "ywnd"],
    gp.ocean.XCurrent: ["x_current", "xcur"],
    gp.ocean.YCurrent: ["y_current", "ycur"],
    gp.ocean.WaterDepth: ["depth", "dpt", "water_depth"],
}

LIST_OF_VAR_ALIASES = [v for v in VAR_ALIASES.values()]


def coord_alias_to_gp(coord_str: str) -> Union[MetaParameter, str]:
    for key, value in COORD_ALIASES.items():
        if coord_str in value:
            return key


def var_alias_to_gp(var_str: str) -> MetaParameter:
    for key, value in VAR_ALIASES.items():
        if var_str in value:
            return key
