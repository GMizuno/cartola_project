import datetime
import glob
import json
import os

import pandas as pd


class Writer:

    def __init__(self, type):
        self.type = type

    @property
    def filename(self):
        if self.type == 'matches':
            return f'{self.type}/{datetime.datetime.now().strftime("%Y")}'
        elif self.type == 'teams':
            return f'{self.type}/{datetime.datetime.now().strftime("%Y")}'
        elif self.type == 'statistics':
            return f'{self.type}/{datetime.datetime.now().strftime("%Y")}'
        else:
            raise ValueError(f'Type {self.type} does not exist')

    def write_json(self, data):
        os.makedirs(os.path.dirname(self.filename), exist_ok=True)
        with open(f'{self.filename}.json', 'w') as f:
            json.dump(data, f)

    def write_json_to_parquet(self, data):
        os.makedirs(os.path.dirname(self.filename), exist_ok=True)
        pd.DataFrame(data).to_parquet(f'{self.filename}.parquet')

    def concat_all_files(self):
        pass
        # TODO: create function to concat all file -> glob.glob(f'{self.filename}/*.parquet', recursive=True)

    def write_json_to_parquet_partition(self, data, partition_col):
        os.makedirs(os.path.dirname(self.filename), exist_ok=True)
        pd.DataFrame(data).to_parquet(f'{self.filename}', partition_cols=partition_col)
