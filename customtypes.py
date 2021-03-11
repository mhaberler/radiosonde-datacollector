# a dict whose values cannot be set to None or nan

import math


class DictNoNone(dict):
    def __setitem__(self, key, value):
        isNAN = isinstance(value, float) and math.isnan(value)
        if value is not None and not isNAN:
            dict.__setitem__(self, key, value)

    def __init__(self, init=None):
        if init is not None:
            for k,v in init.items():
                self.__setitem__(k, v)




