import datetime
import json

from cartola.connector import AwsConnection


class S3Writer:

    def __init__(self, bucket, access_key, secret_access):
        self.bucket = bucket
        self.connection = AwsConnection(access_key, secret_access)

    def get_file_name(self, folder, id):
        if 'gold' in self.bucket:
            return f'{folder}'
        else:
            if id is None:
                return f"""{folder}_{datetime.datetime.now().strftime('%Y-%d-%m')}"""
            return f"""{folder}_{id}_{datetime.datetime.now().strftime('%Y-%d-%m')}"""

    def upload_fileobj(self, data, folder: str, extension: str, **kwargs):
        id = kwargs.get('id')
        filename = self.get_file_name(folder, id)
        if extension == 'parquet':
            data.to_parquet(f's3://{self.bucket}/{folder}/{filename}.{extension}',
                            storage_options=self.connection.storage_option)
        elif extension == 'json':
            self.connection.client.put_object(Body=json.dumps(data), Bucket=self.bucket,
                                              Key=f'{folder}/{filename}.{extension}')
