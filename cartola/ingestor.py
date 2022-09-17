import pandas as pd
import boto3
from pathlib import Path
import datetime
import json


class Ingestor:

    def __init__(self, bucket, access_key, secret_access):
        self.bucket = bucket
        self.storage_option = {'key': access_key,
                               'secret': secret_access}
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_access
        )

    # TODO: Separate Gold from others
    def get_file_name(self, folder):
        return f'{folder}_{datetime.datetime.now().strftime("%Y-%d-%m_%H-%M-%S")}'

    # TODO Improve, does not need to pass all parameters, create method (@proprety) that create bucket and parente_folder
    def upload_fileobj(self, data, folder, extension: str, partition_cols: list = None):
        filename = self.get_file_name(folder)
        if extension == 'parquet':
            if partition_cols is not None:
                data.to_parquet(f's3://{self.bucket}/{folder}/{filename}',
                                storage_options=self.storage_option,
                                partition_cols=partition_cols)
            else:
                data.to_parquet(f's3://{self.bucket}/{folder}/{filename}.{extension}',
                                storage_options=self.storage_option)
        elif extension == 'json':
            self.s3.put_object(Body=json.dumps(data), Bucket=self.bucket, Key=f'{folder}/{filename}.{extension}')

    # TODO Improve, does not need to pass all parameters, create method (@proprety) that create bucket and parente_folder
    def upload_filedisk(self, folder, path: str, **kwargs):
        path = Path(path)
        data = pd.read_parquet(path, **kwargs)

        self.upload_fileobj(data, folder, path.suffix)
