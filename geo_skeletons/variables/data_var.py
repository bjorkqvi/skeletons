from dataclasses import dataclass
from geo_parameters.metaparameter import MetaParameter
import geo_parameters as gp


@dataclass
class DataVar:
    name: str
    meta: MetaParameter
    coord_group: str
    default_value: float
    dir_type: str = None
