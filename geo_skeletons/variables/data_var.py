from dataclasses import dataclass
from geo_parameters.metaparameter import MetaParameter


@dataclass
class DataVar:
    name: str
    meta: MetaParameter
    coord_group: str
    default_value: float
    dir_type: str = None
    rotated_version: "DataVar" = None
    magnitude: str = None
    direction: str = None