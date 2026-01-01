import os
from typing import TypeVar, Dict, Tuple, Generic, Optional, List
from WAL import WAL, replay_wal, new_wal
from MemTable import _MemTable, create
from SSTable import SSTable, write_sstable, TOMBSTONE
from Manifest import Manifest, writemanifest
from Compaction import mergesstable

K = TypeVar('K')
V = TypeVar('V')

class DB(Generic[K, V]):

    def __init__(self,
                 wal: WAL[K, V],
                 walpath: str,
                 memtable: _MemTable[K, V],
                 sstables: Optional[List[SSTable[K, V]]],
                 sstablecounter: int,
                 memtablesize: int,
                 maxmemtablesize: int,
                 manifest: Manifest,
                 manifestpath: str,
                 compactionthreshold: int):

        self.wal = wal
        self.walpath = walpath
        self.memtable = memtable
        self.sstables: Optional[List[SSTable[K, V]]] = (
            sstables) if sstables is not None else []
        self.sstablescounter = sstablecounter
        self.memtablesize = memtablesize
        self.maxmemtablesize = maxmemtablesize
        self.manifest = manifest
        self.manifestpath = manifestpath
        self.compactionthreshold = compactionthreshold

    def put(self, key: K, value: V):
        if self.wal is None:
            wal, err = new_wal(self.walpath)
            if err is not None:
                return err

            self.wal = wal

        # 1. Write to WAL
        try:
            self.wal.write(key, value)
        except Exception as e:
            return e

        # 2. Write to MemTable
        self.memtable.put(key, value)
        self.memtablesize += 1

        if self.memtablesize >= self.maxmemtablesize:
            try:
                self.flushmemtable()
            except Exception as e:
                return e

    def get(self, key: K) -> Tuple[Optional[V], Optional[Exception]]:
        try:
            val, ok = self.memtable.get(key)
            if ok:
                if str(val) == TOMBSTONE:
                    return None, LookupError('Key not found')

                return val, None

            for i in range(len(self.sstables)-1, -1, -1):

                sstable = self.sstables[i]
                val, err = sstable.get(key)
                print('val', val)
                if err:
                    if isinstance(err, EOFError):
                        continue

                    # return None, err
                if val:
                    return val, None
            # Not Found
            return None, LookupError("Key not found")

        except Exception as e:
            raise e

    def flushmemtable(self):
        print('WARNING: FLUSHING MEMTABLE')

        # 1. Rotate WAL
        try:
            self.wal.close()
        except Exception as e:
            return e

        # 2. Flush Memtable to SSTable
        sstablepath = f'.\data\data-{self.sstablescounter}.sstable'
        sstable, err = write_sstable(self.memtable, sstablepath)
        if err:
            print("FAILED TO WRITE SSTABLE: %s", err)
            return err

        self.sstables.append(sstable)
        self.sstablescounter += 1

        if len(self.sstables) >= self.compactionthreshold:
            try:
                self.compact()

            except Exception as e:
                print("COMPACTION FAILED: %s!!!", e)
                return e

        # 3. update and write manifest
        self.manifest.sstablepaths.append(sstablepath)
        try:
            writemanifest(self.manifestpath, self.manifest)
        except Exception as e:
            return e

        # 4. Delete WAL
        try:
            os.remove(self.walpath)
        except Exception as e:
            print("Failed to remove old wal file: %s, %s", self.walpath, e)

        self.wal = None

        # 5. Reset MemTable
        self.memtable = create()
        self.memtablesize = 0

        print("Flushed memtable to %s", sstablepath)


    def delete(self, key: K):
        self.put(key, TOMBSTONE)

    def compact(self):
        print("Starting Compaction...")
        compactedsstablepath = f'.\data\data-compacted-{self.sstablescounter}.sstable'
        compactedsstable, err = (
            mergesstable(self.sstables, compactedsstablepath))


        if err:
            return err

        self.manifest.sstablepaths = [compactedsstablepath]
        print(self.manifest.sstablepaths)
        try:
            writemanifest(self.manifestpath, self.manifest)
        except Exception as e:
            return e

        for sstable in self.sstables:
            try:
                os.remove(sstable.path)
            except Exception as e:
                print('Failed to remove old sstables %s, %s', sstable.path, e)

        self.sstables = [compactedsstable]
        print('Compaction finished')

    def close(self):
        if self.wal:
            return self.wal.close()


def new_db(maxmemtablesize: int, compactionthreshold: int) -> (
    Tuple)[Optional[DB[K, V]], Optional[Exception]]:
    manifestpath = "MANIFEST.manifest"
    manifest, err = Manifest.readmanifest(manifestpath)
    if err:
        return None, err

    sstables = []
    for path in manifest.sstablepaths:
        sstables.append(SSTable(path))

    walpath = '.\data\db.wal'
    memtable, err = replay_wal(walpath)
    if err:
        return None, err

    wal, err = new_wal(walpath)
    if err:
        return None, err

    return DB(wal, walpath, memtable, sstables, len(sstables),
              len(memtable.data), maxmemtablesize, manifest, manifestpath,
              compactionthreshold), None






