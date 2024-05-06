from dataclasses import dataclass
from geo_parameters.metaparameter import MetaParameter


@dataclass
class GridMask:
    name: str
    meta: MetaParameter
    coord_group: str
    default_value: int = None
    primary_mask: bool = True
    opposite_mask: "GridMask" = None
    triggered_by: str = (None,)
    valid_range: tuple[float] = ((0.0, None),)
    range_inclusive: float = (True,)
