from typing import Union


def activate_dask(chunks: Union[tuple[int], str] = "auto"):
    def wrapper(c):
        c._chunks = chunks
        return c

    return wrapper
