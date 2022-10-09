import json
import tempfile
from datetime import datetime
from io import BytesIO

import pandas as pd

from cartola.connector import AwsConnection


class Reader:

    def __init__(self, bucket: str, s3_folder: str, access_key, secret_access):
        self.bucket = bucket
        self.s3_folder = s3_folder
        self.connection = AwsConnection(access_key, secret_access)

    def get_file_name(self):
        return f'{self.s3_folder}_{datetime.now().strftime("%Y-%d-%m_%H-%M-%S")}'

    def get_s3_files(self, suffix: str = '', **kwargs):

        while True:
            resp = self.connection.client.list_objects_v2(Prefix=self.s3_folder, Bucket=self.bucket)
            for obj in resp['Contents']:
                key = obj['Key']
                if key.endswith(suffix):
                    yield key
            try:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            except KeyError:
                break

    def read_file_json(self, **kwargs):
        result = [file for file in self.get_s3_files('json', **kwargs)]
        files = []
        for x in result:
            with tempfile.TemporaryFile() as data:
                self.connection.client.download_fileobj(self.bucket, x, data)
                data.seek(0)
                files += json.loads(data.read().decode('utf-8'))
        return files

    def read_file_parquet(self, **kwargs):
        result = [file for file in self.get_s3_files('parquet', **kwargs)]
        files = []

        for file in result:
            buffer = BytesIO()
            self.connection.client.download_fileobj('bootcamp-silver', file, buffer)
            files.append(buffer)

        return pd.concat([pd.read_parquet(bs) for bs in files])

    def read_file(self, suffix, **kwargs):

        if suffix == 'json':
            return self.read_file_json(**kwargs)
        elif suffix == 'parquet':
            return self.read_file_parquet(**kwargs)
        else:
            raise ValueError('Extension does not supported')
