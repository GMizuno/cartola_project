import datetime
import json
from abc import ABC, abstractmethod

from cartola_project.models import Storage, Bucket, File
from cartola_project.connector import AwsConnection


class S3Writer(ABC):

    def __init__(self, bucket: Bucket, access_key, secret_access):
        self.bucket = bucket
        self.connection = AwsConnection(access_key, secret_access)

    def get_file_name(self, folder: Storage, **kwargs):
        folder_value = folder.value
        bucket_value = self.bucket.value
        league_id = kwargs.get('id')

        if league_id is not None:
            return f"""{folder_value}_{league_id}_{datetime.datetime.now().strftime('%Y-%d-%m_%H:%M:%S')}"""
        return f"""{folder_value}_{datetime.datetime.now().strftime('%Y-%d-%m_%H:%M:%S')}"""


    @abstractmethod
    def upload_fileobj(self, data, folder: Storage, extension: File, **kwargs):
        pass


class S3WriterParquet(S3Writer):

    def __init__(self, bucket: Bucket, access_key, secret_access):
        super().__init__(bucket, access_key, secret_access)

    def upload_fileobj(self, data, folder: Storage, **kwargs):
        filename = self.get_file_name(folder, **kwargs)
        print(f's3://{self.bucket.value}/{folder.value}/{filename}.{File.PARQUET.value}', )
        data.to_parquet(
                f's3://{self.bucket.value}/{folder.value}/{filename}.{File.PARQUET.value}',
                storage_options=self.connection.storage_option
        )


class S3WriterJson(S3Writer):

    def __init__(self, bucket: Bucket, access_key, secret_access):
        super().__init__(bucket, access_key, secret_access)

    def upload_fileobj(self, data, folder: Storage, **kwargs):
        filename = self.get_file_name(folder, **kwargs)
        self.connection.client.put_object(
                Body=json.dumps(data), Bucket=self.bucket.value,
                Key=f'{folder.value}/{filename}.{File.JSON.value}'
        )
