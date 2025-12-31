from typing import Tuple, Dict, Generic, Optional, TypeVar, List, Any
from SSTable import SSTable, Pair, TOMBSTONE
import pickle
import heapq

K = TypeVar('K')
V = TypeVar('V')


class HeapItem(Generic[K, V]):

    def __init__(self, pair: Pair[K, V], sstableindex: int):
        self.pair = pair
        self.sstableindex = sstableindex

    def __lt__(self, other):
        key_i = str(self.pair.key)
        key_j = str(other.pair.key)
        if key_i != key_j:
            return key_i < key_j
        return self.sstableindex > other.sstableindex


class MinHeap(Generic[K, V]):
    def __init__(self):
        self.items: List[HeapItem[K, V]] = []

    def len(self) -> int:
        return len(self.items)

    def push(self, x: HeapItem[K, V]):
        heapq.heappush(self.items, x)

    def pop(self):
        return heapq.heappop(self.items)


def mergesstable(sstables: List[SSTable[K, V]], newpath: str) -> (
    Tuple)[Optional[SSTable[K, V]], Optional[Exception]]:
    try:
        with open(newpath, 'wb') as newfile:

            # newfile = open(newpath, 'wb')
            encoder = pickle.Pickler(newfile)
            files = []
            decoders = []

            # Open all SSTable files
            for i, sstable in enumerate(sstables):
                try:
                    file = open(sstable.path, "rb")
                    files.append(file)
                    decoders.append(pickle.Unpickler(files[i]))

                except OSError as err:
                    # close the file before returning error
                    for opened in files:
                        opened.close()
                    # Equivalent to `return nil, err` in Go
                    return None, err

            pairs = []
            emptysstables = [False] * len(decoders)

            for i, decoder in enumerate(decoders):
                try:
                    pairs.append(decoder.load())
                except EOFError as e:
                    emptysstables[i] = True
                except Exception as e:
                    # close all files before returning error
                    for opened in files:
                        opened.close()
                    return None, e

            # main merge part
            h = MinHeap[K, V]()
            for i, pair in enumerate(pairs):
                if not emptysstables[i] :
                    h.push(HeapItem(pair, i))

            last_key = None
            first_key = True
            while h.len() > 0:
                # Get the smallest pair
                item = h.pop()
                # If this key is a duplicate of the last one we saw, skip it.
                if not first_key and item.pair.key == last_key:
                    pass
                else:
                    if str(item.pair.value) != TOMBSTONE:
                        encoder.dump(item.pair)

                last_key = item.pair.key
                first_key = False

                # Push the next item from the same SSTable into the heap
                try:

                    next_pair = decoders[item.sstableindex].load()
                    h.push(HeapItem(next_pair, item.sstableindex))

                except EOFError:
                    pass
                except Exception as e:
                    return None, e

            return SSTable[K, V](newpath), None

    finally:
        for f in files:
            f.close()
