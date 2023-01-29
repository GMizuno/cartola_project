import json
from abc import abstractmethod, ABC
from io import BytesIO
from pathlib import Path
from typing import Union

import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account

GoogleCredentials = Union[str, dict, service_account.Credentials]
File = Union[dict, list[dict], pd.DataFrame]


class CloudStorage(ABC):

    @abstractmethod
    def client(self):
        pass

    @abstractmethod
    def download(self, *args):
        pass

    @abstractmethod
    def upload(self, *args):
        pass

    @abstractmethod
    def list_files(self, *args):
        pass


# TODO: Add support for Google Cloud Storage without credentials
class GCSStorage(CloudStorage):

    def __init__(self, credentials: GoogleCredentials, project_id: str) -> None:
        self.project_id = project_id
        self._credentials = credentials
        self._client = None

    def get_credentials_from_json(self) -> None:
        with open(self._credentials, 'r') as f:
            file = json.load(f)

        self._credentials = service_account.Credentials.from_service_account_info(
            file
        )

    def get_credentials_from_dict(self) -> None:
        self._credentials = service_account.Credentials.from_service_account_info(
            self._credentials
        )

    @property
    def get_credentials(self) -> GoogleCredentials:
        if isinstance(self._credentials, service_account.Credentials):
            return self._credentials
        elif isinstance(self._credentials, dict):
            self.get_credentials_from_dict()
        elif isinstance(self._credentials, str):
            self.get_credentials_from_json()
        else:
            raise ValueError('Credentials should be either string or dict')
        return self._credentials

    @property
    def client(self) -> storage.Client:
        if self._client is None:
            self._client = storage.Client(project=self.project_id, credentials=self.get_credentials)
        return self._client

    def upload(self, bucket_name: str, file_path: str, file: File) -> None:
        storage_client = self.client
        bucket = storage_client.bucket(bucket_name=bucket_name)
        blob = bucket.blob(file_path)

        if Path(file_path).suffix == '.json':
            file = json.dumps(file)
            blob.upload_from_string(file)
        elif Path(file_path).suffix == '.parquet':
            parquet_bytes = file.to_parquet()
            file = BytesIO(parquet_bytes)
            blob.upload_from_file(file)
        else:
            raise ValueError(f'Unsupported file type: {Path(file_path).suffix}')

    def download(self, bucket_name: str, file_path: str, ) -> bytes:
        storage_client = self.client
        bucket = storage_client.bucket(bucket_name=bucket_name)
        blob = bucket.get_blob(file_path)

        return blob.download_as_bytes()

    def list_files(self, bucket_name: str, file_path: str, ) -> list:
        storage_client = self.client
        blobs = storage_client.list_blobs(bucket_or_name=bucket_name, prefix=file_path)
        return [blob.name for blob in blobs]
