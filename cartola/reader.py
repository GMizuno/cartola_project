import json
import tempfile
from datetime import datetime
from io import BytesIO

import pandas as pd

from cartola.connector import AwsConnection
from cartola.models import Bucket, Storage, File
from abc import ABC, abstractmethod


class Reader(ABC):

    def __init__(self, bucket: Bucket, s3_folder: Storage, access_key, secret_access):
        self.bucket = bucket
        self.s3_folder = s3_folder
        self.connection = AwsConnection(access_key, secret_access)

    def get_file_name(self):
        return f'{self.s3_folder}_{datetime.now().strftime("%Y-%d-%m_%H-%M-%S")}'

    def get_s3_files(self, suffix: File, **kwargs):

        while True:
            resp = self.connection.client.list_objects_v2(Prefix=self.s3_folder.value, Bucket=self.bucket.value)
            for obj in resp['Contents']:
                key = obj['Key']
                if key.endswith(suffix.value):
                    yield key
            try:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            except KeyError:
                break

    @abstractmethod
    def reader(self, **kwargs):
        pass

    def read_file(self, **kwargs):
        return self.reader(**kwargs)


class ReaderParquet(Reader):

    def __init__(self, bucket: Bucket, s3_folder: Storage, access_key, secret_access):
        super().__init__(bucket, s3_folder, access_key, secret_access)

    def reader(self, **kwargs):
        result = [file for file in self.get_s3_files(File.PARQUET, **kwargs)]
        files = []

        for file in result:
            buffer = BytesIO()
            self.connection.client.download_fileobj(Bucket.SILVER, file, buffer)
            files.append(buffer)

        return pd.concat([pd.read_parquet(bs) for bs in files])


class ReaderJson(Reader):

    def __init__(self, bucket: Bucket, s3_folder: Storage, access_key, secret_access):
        super().__init__(bucket, s3_folder, access_key, secret_access)

    def reader(self, **kwargs):
        result = [file for file in self.get_s3_files(File.JSON, **kwargs)]
        files = []
        for x in result:
            with tempfile.TemporaryFile() as data:
                self.connection.client.download_fileobj(self.bucket.value, x, data)
                data.seek(0)
                files += json.loads(data.read().decode('utf-8'))
        return files

