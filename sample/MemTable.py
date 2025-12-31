from __future__ import annotations
from typing import TypeVar, Generic, Dict, Tuple, Optional
import traceback

K = TypeVar('K')
V = TypeVar('V')


class _MemTable(Generic[K, V]):

    def __init__(self):
        # variable: dtype = empty value
        self.data: Dict[K, V] = {}

    def put(self, key, value):
        # put function to add new key and value
        self.data[key] = value

    def get(self, key: K) -> Tuple[Optional[V], bool]:
        # get function to check key is available
        if key in self.data:
            return self.data[key], True
        else:
            # Since, Python is an interpreted language & it won't
            # throw error if dtype mismatch occurs, we need to pass a default
            # value based on the key passed
            def zero_value(t):
                try:
                    return t()

                except (AttributeError, ValueError) as e:
                    return traceback.walk_tb(e.__traceback__)

            return zero_value(type(key)), False

    def get_all(self):
        if self.data:
            return self.data
        else:
            return None

    def len(self):
        return len(self.data)


def create() -> _MemTable[K, V]:
    """
    Factory method that creates and returns a new instance for _MemTable.
    """
    return _MemTable()

