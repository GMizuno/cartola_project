from abc import ABC, abstractmethod

import pandas as pd

from cartola_project.storage.storage import Storage

File = dict | list[dict] | pd.DataFrame


class Writer(ABC):
    @abstractmethod
    def write(self):
        pass


class JSONWriter(Writer):
    def __init__(
        self,
        storage: Storage,
        file_path: str,
        file: File,
    ):
        self.storage = storage
        self.file_path = file_path
        self.file = file

    def write(self) -> None:
        self.storage.upload(self.file_path, self.file)


class ParquetWriter(Writer):
    def __init__(
        self,
        storage: Storage,
        file_path: str,
        file: File,
    ):
        self.storage = storage
        self.file_path = file_path
        self.file = file

    def write(self) -> None:
        self.storage.upload(self.file_path, self.file)
