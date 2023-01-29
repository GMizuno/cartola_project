from abc import ABC, abstractmethod

from cartola_project.connector import CloudStorage, File


class Writer(ABC):

    @abstractmethod
    def write(self):
        pass


class JSONWriter(Writer):

    def __init__(self, cloud_storage: CloudStorage, bucket_name: str, file_path: str, file: File):
        self.cloud_storage = cloud_storage
        self.bucket_name = bucket_name
        self.file_path = file_path
        self.file = file

    def write(self) -> None:
        self.cloud_storage.upload(self.bucket_name, self.file_path, self.file)


class ParquetWriter(Writer):

    def __init__(self, cloud_storage: CloudStorage, bucket_name: str, file_path: str, file: File):
        self.cloud_storage = cloud_storage
        self.bucket_name = bucket_name
        self.file_path = file_path
        self.file = file

    def write(self) -> None:
        self.cloud_storage.upload(self.bucket_name, self.file_path, self.file)
