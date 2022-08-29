import pandas as pd
import pandas_gbq as pd_gbq
from io import BytesIO, StringIO

class Ingestor:

    def __init__(self, destination_table, project_id):
        self.destination_table = destination_table
        self.project_id = project_id

    def send_to_bigquery(self, file_path, if_exists: str = 'replace'):
        data = pd.read_parquet(file_path)
        pd_gbq.to_gbq(data, self.destination_table, self.project_id, if_exists=if_exists)



