import pandas as pd
from decouple import config

import dask.dataframe as dd

d = dd.read_parquet('s3://bootcamp-silver/matches/*.parquet')
list(set(d.id_team_home.drop_duplicates().compute().to_list() + \
         d.id_team_away.drop_duplicates().compute().to_list()))
