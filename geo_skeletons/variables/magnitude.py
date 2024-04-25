from dataclasses import dataclass
from geo_parameters.metaparameter import MetaParameter


@dataclass
class Magnitude:
    name: str
    meta: MetaParameter
    x: str
    y: str
    coord_group: str
    direction: "Direction" = None


@dataclass
class Direction:
    name: str
    meta: MetaParameter
    x: str
    y: str
    coord_group: str
    dir_type: str = None
    magnitude: Magnitude = None
