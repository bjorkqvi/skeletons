from dataclasses import dataclass
from geo_parameters.metaparameter import MetaParameter


@dataclass
class Coordinate:
    name: str
    meta: MetaParameter
    coord_group: str
