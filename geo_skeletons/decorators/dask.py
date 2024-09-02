from typing import Union
from copy import deepcopy


def activate_dask(chunks: Union[tuple[int], str] = "auto"):
    def wrapper(c):
        c._chunks = deepcopy(chunks)
        return c

    return wrapper
