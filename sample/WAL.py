from typing import Tuple, Optional, Generic, TypeVar, Dict, BinaryIO
from MemTable import _MemTable
from dataclasses import dataclass
import pickle


K = TypeVar('K')
V = TypeVar('V')


@dataclass
class WALEntry(Generic[K, V]):
    Key: K
    Value: V


class WAL(Generic[K, V]):
    def __init__(self, file: BinaryIO):
        self.__file = file

    def write(self, key: K, value: V):
        entry: WALEntry[K, V] = WALEntry(key, value)
        return pickle.dump(entry, self.__file)

    def close(self):
        return self.__file.close()


def new_wal(path: str) -> Tuple[Optional[WAL[K, V]], Optional[Exception]]:
    try:
        file = open(path, 'ab')

    except Exception as e:
        return None, e

    return WAL(file), None


def replay_wal(path: str) -> Tuple[Optional[_MemTable[K, V]], Optional[Exception]]:

    memtable = _MemTable()
    try:

        with open(path, 'rb') as file:
            while True:
                try:
                    entry: WALEntry[K, V] = pickle.load(file)

                except EOFError:
                    # When EOF occurs we should return the memtable
                    break

                memtable.put(entry.Key, entry.Value)

    except FileNotFoundError as e:
        return memtable, None

    except pickle.UnpicklingError as e:
        return None, e

    except Exception as e:
        return None, e

    return memtable, None










