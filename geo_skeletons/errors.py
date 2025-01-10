from typing import Union


class SkeletonError(Exception):
    pass


class StaticSkeletonError(SkeletonError):
    def __init(self):
        super().__init__(
            "Cannot add variable to a static Skeleton! Use @dynamic to change class or set instance core to obj.core.static = False"
        )


class UnknownCoordinateError(SkeletonError):
    pass


class UnknownVariableError(SkeletonError):
    pass


class CoordinateWrongLengthError(SkeletonError):
    def __init__(
        self,
        variable: str,
        len_of_variable: int,
        index_variable: str,
        len_of_index_variable: int,
    ):
        super().__init__(
            f"Variable {variable} is {len_of_variable} long but variable {index_variable} is {len_of_index_variable} long!"
        )


class DataWrongDimensionError(SkeletonError):
    def __init__(self, data_shape: tuple[int], coord_shape: Union[tuple[int], int]):
        if isinstance(coord_shape, int):  # Only amount of coords given
            super().__init__(
                f"Data has shape {data_shape}, but only {coord_shape} coordinates provided!!!"
            )
        else:
            super().__init__(
                f"Data has shape {data_shape}, but coordinates define a shape {coord_shape}!!!"
            )


class CoordinateWrongDimensionError(SkeletonError):
    def __init__(self, coord_name: str, coord_shape: tuple[int]):
        super().__init__(
            f"Coordinate {coord_name} has shape {coord_shape}, but it should have only one dimension!!!"
        )


class GridError(SkeletonError):
    def __init__(
        self,
        msg: str = "A proper spatial grid is not set: Requires 'x' and 'y', 'lon' and 'lat' or 'inds'!",
    ):
        super().__init__(msg)


class VariableExistsError(SkeletonError):
    def __init__(self, var_name):
        super().__init__(f"'{var_name}' has already been added to the class!!!")


class DirTypeError(SkeletonError):
    def __init__(self):
        super().__init__(
            f"Cannot use 'dir_type' with a non-directional variable or a magnitude!!!"
        )
