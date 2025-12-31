import os
from typing import TypeVar, List, Optional, Generic, Dict, Tuple
import pickle

K = TypeVar('K')
V = TypeVar('V')


class Manifest:

    def __init__(self, sstablepaths: List[str]):
        self.sstablepaths = sstablepaths

    @classmethod
    def readmanifest(cls, path: str) -> (
            Tuple)[Optional['Manifest'], Optional[Exception]]:
        try:

            with open(path, 'rb') as file:
                try:
                    manifest: Manifest = pickle.load(file)
                    return manifest, None
                except EOFError as e:
                    # Similar to Manifest([])
                    return cls([]), e
                except Exception as e:
                    return None, e

        except FileNotFoundError:
            # Similar to Manifest([])
            return cls([]), None

        except Exception as e:
            return None, e


def writemanifest(path: str, manifest: Manifest):
    tmppath = path + '.tmp'
    try:
        with open(tmppath, 'wb')as file:
            try:

                pickle.dump(manifest, file)

            except Exception as e:
                try:
                    os.remove(tmppath)
                except FileNotFoundError as e:
                    pass
                return e
        # If destination exists it will overwrite
        os.replace(tmppath, path)
    except Exception as e:
        return e

    return None








