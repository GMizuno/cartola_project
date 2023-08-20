import json
from abc import ABC, abstractmethod
from io import BytesIO
from pathlib import Path

import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account

GoogleCredentials = str | dict | service_account.Credentials
File = dict | list[dict] | pd.DataFrame


class Storage(ABC):
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


class GCSStorage(Storage):
    def __init__(self, credentials: GoogleCredentials, project_id: str, bucket_name: str) -> None:
        self.project_id = project_id
        self._credentials = credentials
        self.bucket_name = bucket_name
        self._client = None

    def get_credentials_from_json(self) -> None:
        with open(self._credentials, "r") as f:
            file = json.load(f)

        self._credentials = service_account.Credentials.from_service_account_info(file)

    def get_credentials_from_dict(self) -> None:
        self._credentials = service_account.Credentials.from_service_account_info(self._credentials)

    @property
    def get_credentials(self) -> GoogleCredentials:
        if isinstance(self._credentials, service_account.Credentials):
            return self._credentials
        elif isinstance(self._credentials, dict):
            self.get_credentials_from_dict()
        elif isinstance(self._credentials, str):
            self.get_credentials_from_json()
        elif self._credentials is None:
            print("Using default inferred from the environment")
        else:
            raise ValueError("Credentials should be either string or dict")
        return self._credentials

    @property
    def client(self) -> storage.Client:
        if self._client is None:
            self._client = storage.Client(project=self.project_id, credentials=self.get_credentials)
        return self._client

    def _get_file_data(self, file_path: str, file: File) -> str | bytes:
        suffix = Path(file_path).suffix
        if suffix == ".json":
            return json.dumps(file)
        elif suffix == ".parquet":
            return file.to_parquet()
        else:
            raise ValueError(f"Unsupported file type: {suffix}")

    def upload(
        self,
        file_path: str,
        file: File,
    ) -> None:
        storage_client = self.client
        bucket = storage_client.bucket(bucket_name=self.bucket_name)
        blob = bucket.blob(file_path)

        file_data = self._get_file_data(file_path, file)
        if isinstance(file_data, str):
            blob.upload_from_string(file_data)
        else:
            blob.upload_from_file(BytesIO(file_data))

    def download(
        self,
        file_path: str,
    ) -> bytes:
        storage_client = self.client
        bucket = storage_client.bucket(bucket_name=self.bucket_name)
        blob = bucket.get_blob(file_path)

        return blob.download_as_bytes()

    def list_files(
        self,
        file_path: str,
    ) -> list:
        storage_client = self.client
        blobs = storage_client.list_blobs(bucket_or_name=self.bucket_name, prefix=file_path)
        return [blob.name for blob in blobs]


class LocalStorage(Storage):
    def __init__(self):
        self._client = None
        self.is_cloud = False

    def client(self):
        print(f'You are using local storage')
        return self._client

    def upload(self, file_path: str, file: File):
        file_path = Path(file_path)

        if file_path.suffix == ".json":
            file = json.dumps(file)
            with open(file_path, "w") as f:
                f.write(file)
        elif file_path.suffix == ".parquet":
            return file.to_parquet()
        else:
            raise ValueError(f"Unsupported file type: {file_path.suffix}")

    def download(self, file_path: str = '.'):
        file_path = Path(file_path)

        if file_path.suffix == '.parquet':
            return pd.read_parquet(file_path)
        elif file_path.suffix == '.parquet':
            with open(file_path, 'rb') as f:
                file = f.read()
                return json.loads(file)
        else:
            raise ValueError(f"Unsupported file type: {file_path.suffix}")

    def list_files(self, file_path):
        path = Path(file_path)
        return [item for item in path.iterdir() if item.is_file()]
