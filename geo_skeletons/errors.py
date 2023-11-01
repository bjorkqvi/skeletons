class UnknownCoordinateError(Exception):
    pass

class CoordinateWrongLengthError(Exception):
    def __init__(self, variable: str, len_of_variable: int, index_variable: str, len_of_index_variable: int):
        super().__init__(f"Variable {variable} is {len_of_variable} long but the index variable {index_variable} is {len_of_index_variable} long!")

class DataWrongDimensionError(Exception):
    def __init__(self, data_shape: tuple[int], coord_shape: tuple[int]):
        super().__init__(f"Data has shape {data_shape}, but coordinates define a shape {coord_shape}!!!")

class GridError(Exception):
    def __init__(self):
        super().__init__("A proper spatial grid is not set: Requires 'x' and 'y', 'lon' and 'lat' or 'inds'!")