import json
from abc import ABC, abstractmethod
from io import BytesIO

import pandas as pd

from cartola_project.connector import CloudStorage


class Reader(ABC):

    @abstractmethod
    def read(self):
        pass

    @abstractmethod
    def read_all_files(self):
        pass


class JSONReader(Reader):

    def __init__(self, cloud_storage: CloudStorage, bucket_name: str, file_path: str):
        self.cloud_storage = cloud_storage
        self.bucket_name = bucket_name
        self.file_path = file_path

    def read(self) -> dict:
        file = self.cloud_storage.download(self.bucket_name, self.file_path)
        return json.loads(file.decode('utf-8'))

    def read_all_files(self) -> list:
        files = self.cloud_storage.list_files(self.bucket_name)


class ParquetReader(Reader):

    def __init__(self, cloud_storage: CloudStorage, bucket_name: str, file_path: str):
        self.file_path = file_path
        self.bucket_name = bucket_name
        self.cloud_storage = cloud_storage

    def read(self) -> pd.DataFrame:
        file = self.cloud_storage.download(self.bucket_name, self.file_path)
        pq_file = BytesIO(file)
        return pd.read_parquet(pq_file)
