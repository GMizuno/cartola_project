import pandas as pd
import pandas_gbq as pd_gbq
from google.cloud import storage


class Ingestor:

    def __init__(self, destination_table, project_id):
        self.destination_table = destination_table
        self.project_id = project_id
        self.client = storage.Client()

    def list_file_bucket(self, bucket: str, folder: str):
        return [blob.name for blob in self.client.list_blobs(bucket, prefix=f'f{folder}/') if
                blob.name.endswith('.parquet')]

    def path_pandas_parquet(self, bucket: str, folder: str):
        return [f'gcs://{bucket}/' + file_path for file_path in self.list_file_bucket(bucket, folder)]

    def send_local_file_to_bigquery(self, file_path: str, if_exists: str = 'replace'):
        data = pd.read_parquet(file_path)
        pd_gbq.to_gbq(data, self.destination_table, self.project_id, if_exists=if_exists)
