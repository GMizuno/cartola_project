import requests
from decouple import config
import pandas as pd
from typing import List, Dict

class Fixtures():
    HEADERS = headers = {
        "X-RapidAPI-Host": config('API_HOST_KEY'),
        "X-RapidAPI-Key": config('API_SECERT_KEY')
    }
    URL = 'https://api-football-v1.p.rapidapi.com/v3/fixtures'

    def __init__(self, league_id: str, season: str) -> None:
        self.query_parametres = {"league":league_id,"season":season}

    def check_request(self) -> Dict:
        response = requests.request("GET", Fixtures.URL, headers=Fixtures.HEADERS, params=self.query_parametres)

        if response.status_code == 200:
            return response.json()
        else:
            print(f'Http status {response}')

    def fixtures(self) -> pd.DataFrame:
        response = self.check_request()
        ite = response.get("response")

        fixtures_df = [
            {'partida_id': value.get('fixture').get('id'),
             'date': value.get('fixture').get('date'),
             'rodada': value.get('league').get('round'),
             'league_id': value.get('league').get('id')
             } for index, value in enumerate(ite)]

        fixtures_df = pd.DataFrame(fixtures_df)
        fixtures_df = fixtures_df.astype(
            {'date': 'datetime64[ns]', 'partida_id': 'int', 'rodada': 'str'}
        )

        fixtures_df = fixtures_df. \
            groupby(['rodada']). \
            agg(final_rodada=('date', 'max'), inicio_rodada=('date', 'min')).reset_index()

        return fixtures_df

class Teams():
    HEADERS = headers = {
        "X-RapidAPI-Host": config('API_HOST_KEY'),
        "X-RapidAPI-Key": config('API_SECERT_KEY')
    }
    URL = "https://api-football-v1.p.rapidapi.com/v3/teams"

    def check_request(self, team_id: str):
        self.querystring = {"id": team_id}
        response = requests.request("GET", Teams.URL, headers=Teams.HEADERS, params=self.querystring)

        if response.status_code == 200:
            return response
        else:
            print(f'Http status {response}')

    def get_team(self, id: str) -> Dict[str, str]:
        response = self.check_request(id)
        response_json = response.json()

        return {
            'team_id': response_json.get('parameters').get('id'),
            'name': response_json.get('response')[0].get('team').get('name'),
            'code': response_json.get('response')[0].get('team').get('code'),
            'country': response_json.get('response')[0].get('team').get('country'),
            'city': response_json.get('response')[0].get('venue').get('city'),
            'logo': response_json.get('response')[0].get('team').get('logo')
        }

    def get_teams(self, id_list: List[str]) -> pd.DataFrame:
        return pd.DataFrame(self.get_team(id) for id in id_list)

class Match():
    pass

class Time():
    pass

class Fact_Team():
    pass
