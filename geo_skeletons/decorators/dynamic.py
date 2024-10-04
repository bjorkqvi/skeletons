from copy import deepcopy


def dynamic(c):
    c.core = deepcopy(c.core)
    c.meta = c.core.meta
    c.core.static = False
    return c
