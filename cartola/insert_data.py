import pandas as pd
from decouple import config

conn_string = f"postgresql://{config('POSTGRES_USER')}:{config('POSTGRES_PASS')}@localhost:5432/cartola"

times = pd.read_parquet('transformed/Teams.parquet')
times = times.astype({'team_id':'int64'})
times.dtypes

times.to_sql(name='times', con=conn_string,schema='public',if_exists='replace',index=False)

partidas = pd.read_parquet('transformed/Fixtures.parquet')
partidas.dtypes

partidas.to_sql(name='partidas', con=conn_string,schema='public',if_exists='replace',index=False)