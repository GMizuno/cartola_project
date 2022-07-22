import requests
from decouple import config
import json
import pandas as pd
import re
import time
import datetime as dt
from pathlib import Path


class Fixtures():
    HEADERS = headers = {
        "X-RapidAPI-Host": config('API_HOST_KEY'),
        "X-RapidAPI-Key": config('API_SECERT_KEY')
    }
    URL = 'https://api-football-v1.p.rapidapi.com/v3/fixtures'

    def __init__(self, query_parameter):
        self.query_parametres = query_parameter

    def check_request(self):

        response = requests.request("GET", Fixtures.URL, headers=Fixtures.HEADERS, params=self.query_parametres)

        if response.status_code == 200:
            return response
        else:
            print(f'Http status {response}')

    def fixtures(self, path_file_save='raw'):
        response = self.check_request()
        print(response)
        response_json = response.json()
        p = Path(path_file_save)

        fixtures_df = [
            {'partida_id': value.get('fixture').get('id'),
             'date': value.get('fixture').get('date'),
             'rodada': value.get('league').get('round'),
             'league_id': self.query_parametres.get('league')
             } for index, value in enumerate(response_json.get("response"))]

        fixtures_df = pd.DataFrame(fixtures_df)
        fixtures_df = fixtures_df.astype(
            {'date': 'datetime64[ns]', 'partida_id': 'int', 'rodada': 'str'}
        )

        fixtures_df = fixtures_df. \
            groupby(['rodada']). \
            agg(final_rodada=('date', 'max'), inicio_rodada=('date', 'min')).reset_index()

        if p.exists():
            fixtures_df.to_parquet(f'{path_file_save}/Fixtures.parquet')
        else:
            print('path doesnt exist')
