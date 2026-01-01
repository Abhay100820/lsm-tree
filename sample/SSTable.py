from typing import TypeVar, Generic, Optional, Dict, Tuple
from sample.MemTable import _MemTable
import traceback
from dataclasses import dataclass
import pickle

K = TypeVar('K')
V = TypeVar('V')
TOMBSTONE = "__TOMBSTONE__"


class SSTable(Generic[K, V]):

    def __init__(self, path: str):
        self.path = path

    def get(self, key: K) -> (
            Tuple)[Optional[V], Optional[Exception]]:
        try:

            with open(self.path, 'rb') as file:
                while True:
                    print('YES')
                    print(self.path)
                    try:

                        pair: Pair[K, V] = pickle.load(file)

                    except EOFError as e:
                        print(e)
                        traceback.print_tb(e.__traceback__)
                        return None, e
                    print('PAIR: ', pair)
                    key_in_db = pair.key
                    print('pair.key', pair.key)
                    if key_in_db == key:
                        if pair.value == TOMBSTONE:
                            return None, LookupError('Key not found')

                        return pair.value, None
                    # Table is sorted and if we have passed the key we can
                    # return None
                    # if pair.key > key:
                    #     return None, LookupError('Key not found')

        except Exception as e:
            return None, e


@dataclass
class Pair(Generic[K, V]):
    key: K
    value: V


def write_sstable(memtable: _MemTable[K, V], path: str) -> (
        Tuple)[Optional[SSTable[K, V]], Optional[Exception]]:
    # try:
    #     open(path, 'w').close()
    # except Exception as e:
    #     return None, e

    pairs = [Pair(key, value) for key, value in memtable.get_all().items()]

    pairs.sort(key=lambda p: str(p.key))
    with open(path, 'wb') as file:
        for pair in pairs:
            try:

                pickle.dump(pair, file)

            except Exception as e:
                return None, e

    return SSTable(path), None
