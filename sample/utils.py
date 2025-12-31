from sample.MemTable import _MemTable
from sample.SSTable import SSTable, write_sstable
from typing import TypeVar


K = TypeVar('K')
V = TypeVar('V')


class DB:

    @staticmethod
    def create() -> _MemTable[K, V]:
        """
        Factory method that creates and returns a new instance for _MemTable.
        """
        return _MemTable()


mt = DB.create()
mt.put('abhay', 10)
mt.put('aditya', 'pathak')
mt.put('keshav', True)

path = r'C:\Users\abhay\PycharmProjects\LSM_TREE\sample\test.txt'
sst = write_sstable(mt, path)
st = SSTable(path)
print(st.get('abhay'))
print(st.get('nihal'))
print(mt.get_all())
