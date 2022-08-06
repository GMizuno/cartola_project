import requests
from decouple import config
import pandas as pd
from util import check_list
import json


class Requester():

    def __init__(self, api_host_key, api_secert_key, params, type):
        self.headers = headers = {
            "X-RapidAPI-Host": api_host_key,
            "X-RapidAPI-Key": api_secert_key
        }
        self.type = type
        self.params = params

        if self.type == 'fixture' and isinstance(self.type, str):
            self.url = 'https://api-football-v1.p.rapidapi.com/v3/fixtures'
        elif self.type == 'match' and isinstance(self.type, str):
            self.url = "https://api-football-v1.p.rapidapi.com/v3/fixtures/statistics"
        elif self.type == 'team' and isinstance(self.type, str):
            self.url = "https://api-football-v1.p.rapidapi.com/v3/fixtures/statistics"
        elif self.type == 'roudn' and isinstance(self.type, str):
            self.url = "https://api-football-v1.p.rapidapi.com/v3/fixtures/statistics"
        else:
            raise TypeError(f'Type {self.type} does not exist, try other')

    def check_params(self):

        if self.type == 'fixture':
            return all([isinstance(param, str) for param in self.params.values()]) and \
                   set(self.params.keys()) == {'season', 'league'}

        elif self.type == 'match' or self.type == 'round':
            if set(self.params.keys()) == {'id'}:
                if isinstance(list(self.params.values())[0], list):
                    return check_list(list(self.params.values())[0], str)
                else:
                    return all([isinstance(param, str) for param in self.params.values()])
            raise ValueError(f'params {self.params} is not correct, please pass other parameter ')

        elif self.type == 'team':
            if set(self.params.keys()) == {'team'}:
                if isinstance(list(self.params.values())[0], list):
                    return check_list(list(self.params.values())[0], str)
                else:
                    return all([isinstance(param, str) for param in self.params.values()])
            raise ValueError(f'params {self.params} is not correct, please pass other parameter ')

    def check_request(self):

        if self.check_params():
            response = requests.request("GET", self.url, headers=self.headers, params=self.params)

            if response.status_code == 200:
                return response.json()
            else:
                response.raise_for_status()
        raise ValueError(f'Parameters types are wrong please correct')


class Fixtures(Requester):

    def __init__(self, api_host_key, api_secert_key, params, type):
        Requester.__init__(self, api_host_key, api_secert_key, params, type)

    def fixtures_json(self):
        response = self.check_request()
        print(response)
        ite = response.get("response")

        fixtures_json = [
            {'partida_id': value.get('fixture').get('id'),
             'date': value.get('fixture').get('date'),
             'rodada': value.get('league').get('round'),
             'league_id': value.get('league').get('id'),
             'id_team_away': value.get('teams').get('away').get('id'),
             'id_team_home': value.get('teams').get('home').get('id'),
             } for index, value in enumerate(ite)]

        return fixtures_json

    def fixtures_dataframe(self):
        return pd.DataFrame(self.fixtures_json())