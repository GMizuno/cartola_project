import json
from abc import ABC, abstractmethod
from io import BytesIO

import pandas as pd

from cartola_project.storage import CloudStorage


class Reader(ABC):
    @abstractmethod
    def read(self):
        """Abstart method to read the data from the file."""
        raise NotImplementedError

    @abstractmethod
    def read_all_files(self, *args):
        """Abstart method to read all the data from list of files.

        Args:
            *args:
        """
        raise NotImplementedError


class JSONReader(Reader):
    """Concrete class to read data from a JSON file."""

    def __init__(
        self,
        cloud_storage: CloudStorage,
        bucket_name: str,
        file_path: str,
    ):
        self.cloud_storage = cloud_storage
        self.bucket_name = bucket_name
        self.file_path = file_path

    def read(self) -> dict:
        """Download the data from the storage.

        Returns:
            Json object.

        """
        file = self.cloud_storage.download(
            self.bucket_name,
            self.file_path,
        )
        return json.loads(file.decode("utf-8"))

    def read_all_files(self) -> list[dict]:
        """Read all the data from the storage.

        Returns:
            List of Json objects.
        """
        files = self.cloud_storage.list_files(
            self.bucket_name,
            self.file_path,
        )
        files_download = [
            self.cloud_storage.download(
                self.bucket_name,
                file,
            )
            for file in files
        ]
        return [json.loads(file.decode("utf-8")) for file in files_download]


class ParquetReader(Reader):
    """Concrete class to read data from a Parquet file."""

    def __init__(
        self,
        cloud_storage: CloudStorage,
        bucket_name: str,
        file_path: str,
    ):
        self.file_path = file_path
        self.bucket_name = bucket_name
        self.cloud_storage = cloud_storage

    def read(self) -> pd.DataFrame:
        """Read the data from the storage.
        Use pandas tod read the data and return a pandas DataFrame

        Returns:
            Pandas DataFrame.
        """
        file = self.cloud_storage.download(
            self.bucket_name,
            self.file_path,
        )
        pq_file = BytesIO(file)
        return pd.read_parquet(pq_file)

    def read_all_files(self) -> pd.DataFrame:
        """Read all the data from the storage.
        Use pandas tod read the data and return a pandas DataFrame

        Returns:
            Pandas DataFrame.
        """
        files = self.cloud_storage.list_files(
            self.bucket_name,
            self.file_path,
        )

        print(files)

        files_download = [
            self.cloud_storage.download(
                self.bucket_name,
                file,
            )
            for file in files
        ]
        return pd.concat([pd.read_parquet(BytesIO(file)) for file in files_download])
