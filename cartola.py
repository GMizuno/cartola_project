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
        self.query_parametres = {"league": league_id, "season": season}

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
             'league_id': value.get('league').get('id'),
             'id_team_away': value.get('teams').get('away').get('id'),
             'id_team_home': value.get('teams').get('home').get('id'),
             } for index, value in enumerate(ite)]

        return pd.DataFrame(fixtures_df)

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

    def __init__(self, start_date: str, end_date: str) -> None:
        self.start_date = start_date
        self.end_date = end_date

    def create_time_df(self) -> pd.DataFrame:
        time_df = pd.DataFrame()
        time_df['date'] = pd.date_range(start=self.start_date, end=self.end_date)
        time_df['day'] = time_df['date'].dt.day
        time_df['month'] = time_df['date'].dt.month
        time_df['year'] = time_df['date'].dt.year
        time_df['day_of_week'] = time_df['date'].dt.dayofweek
        time_df['quarter'] = time_df['date'].dt.quarter
        time_df['month_name'] = time_df['date'].dt.month_name()
        time_df['day_name'] = time_df['date'].dt.day_name()

        return time_df

class Matches():
    HEADERS = headers = {
        "X-RapidAPI-Host": config('API_HOST_KEY'),
        "X-RapidAPI-Key": config('API_SECERT_KEY')
    }
    URL = "https://api-football-v1.p.rapidapi.com/v3/fixtures/statistics"

    def check_request(self, match_id: str) -> None:
        self.querystring = {"fixture": f'{match_id}'}
        response = requests.request("GET", Matches.URL, headers=Matches.HEADERS, params=self.querystring)

        if response.status_code == 200:
            return response
        else:
            print(f'Http status {response}')

    def get_match(self, match_id: str) -> Dict[str, List[str|int|float]]:
        response = self.check_request(match_id)
        response_json = response.json()

        stats_fixure = {'Shots on Goal': [], 'Shots off Goal': [], 'Total Shots': [], 'Blocked Shots': [],
                        'Shots insidebox': [], 'Shots outsidebox': [], 'Fouls': [], 'Corner Kicks': [],
                        'Offsides': [], 'Ball Possession': [], 'Yellow Cards': [], 'Red Cards': [],
                        'Goalkeeper Saves': [], 'Total passes': [], 'Passes accurate': [], 'Passes %': [],
                        'match_id': [], 'team_id': []}

        for informations in response_json['response']:
            for information in informations['statistics']:
                stats_fixure[f"{information['type']}"].append(information['value'])
            stats_fixure['team_id'].append(informations['team']['id'])
            stats_fixure['match_id'].append(response_json['parameters']['fixture'])

        return stats_fixure

    def get_multiple_match(self, fixtures_id: List[str]) -> pd.DataFrame:
        return pd.concat([pd.DataFrame(self.get_match(id)) for id in fixtures_id])