from abc import abstractmethod, ABC

import pandas as pd

from cartola_project.models import (Club, Match, )
from cartola_project.transformations.util import (convert_time, clean_dict_key,
                                                  convert_date, flatten_dict, )


class Transformer(ABC):

    @abstractmethod
    def _get_transformation(self):
        pass

class FixturesTransformer(Transformer):

    def __init__(self, file: dict) -> None:
        self.file = file

    def extract_model(self):
        response = self.file[0].get('response')

        return [Match.from_dict(fixture) for fixture in response]

    def to_dataframe(self, list_model: list):
        data = pd.DataFrame([clean_dict_key(i) for i in list_model])

        data.rename(columns={'partida_id': 'match_id', 'rodada': 'round'},
                    inplace=True)
        data.replace(to_replace=r'Regular Season - ', value='', regex=True,
                     inplace=True)
        data.replace(to_replace=r'Group Stage - ', value='', regex=True,
                     inplace=True)

        return data.drop_duplicates()

    def _get_transformation(self) -> pd.DataFrame:
        fixture_json = []

        for fixture in self.extract_model():
            result = {'partida_id': fixture.fixture.id,
                      'date': convert_time(fixture.fixture.date),
                      'reference_date': convert_date(fixture.fixture.date),
                      'rodada': fixture.league.round,
                      'league_id': fixture.league.id,
                      'id_team_away': fixture.teams.away.id,
                      'id_team_home': fixture.teams.home.id,
                      'goals_home': fixture.goals.home,
                      'goals_away': fixture.goals.away,
                      'winner_home': fixture.teams.home.winner,
                      'winner_away': fixture.teams.away.winner,
                      }
            fixture_json.append(result)

        return self.to_dataframe(fixture_json)

class TeamsTransformer(Transformer):

    def __init__(self, file: dict) -> None:
        self.file = file

    def extract_model(self):
        response = map(lambda x: x.get('response'), self.file)
        return map(lambda x: Club.from_dict(x[0]), response)

    def to_dataframe(self, list_model: list) -> pd.DataFrame:
        data = pd.DataFrame([clean_dict_key(i) for i in list_model])

        data_location = data['city'].str.split(',', 1, expand=True)
        data_location.rename(columns={0: 'city', 1: 'state'}, inplace=True)
        data_location['state'] = data_location['state'].fillna(
            data_location['city'])
        data = data[['team_id', 'name', 'code', 'country', 'logo']]
        data = pd.concat([data, data_location], axis=1)

        return data.drop_duplicates()

    def _get_transformation(self) -> pd.DataFrame:
        teams_json = []

        for club in self.extract_model():
            teams_json.append({
                'team_id': int(club.team.id),
                'name': club.team.name,
                'code': club.team.name,
                'country': club.team.name,
                'city': club.venue.city,
                'logo': club.team.logo,
            }
            )

        return self.to_dataframe(teams_json)

class MatchTransformer(Transformer):

    def __init__(self, file: dict) -> None:
        self.file = file

    def _get_transformation(self) -> pd.DataFrame:

        stats_json = []

        for informations in self.file:
            for i in informations.get('response'):
                stats_matches = {}
                for j in i.get('statistics'):
                    stats_matches.update({j.get('type'): str(j.get('value'))})
                team_id = i.get('team')['id']
                match_id = informations.get('parameters').get('fixture')
                stats_matches.update({'team_id': str(team_id), 'match_id': str(match_id)})
                stats_json.append(stats_matches)

        data = pd.DataFrame([clean_dict_key(i) for i in stats_json])

        data.replace('None', 0, inplace=True)
        data.replace(to_replace=r'%', value='', regex=True, inplace=True)
        data = data.astype(int)
        data['Passes_percentage'] = data['Passes_percentage'].div(100)
        data['Ball_Possession'] = data['Ball_Possession'].div(100)

        return data.drop_duplicates()

class PlayerTransformer(Transformer):

    def __init__(self, file: dict) -> None:
        self.file = file

    @staticmethod
    def parameters(data: dict) -> dict:
        return data.get('parameters', None)

    @staticmethod
    def response(data: dict) -> list[dict]:
        return data.get('response', None)

    @staticmethod
    def get_team(data: dict) -> dict:
        return data.get('team', None)

    @staticmethod
    def get_players(data: dict) -> dict:
        return data.get('players', None)

    @staticmethod
    def get_player_info(data: dict) -> list[dict]:
        return list(map(lambda x: flatten_dict(x.get('player')), data))

    @staticmethod
    def get_player_stats(data: dict) -> list[dict]:
        return list(map(lambda x: flatten_dict(x.get('statistics')[0]), data))

    @staticmethod
    def build_json(data: dict) -> list[dict]:
        restult = []
        for i in data.get('response'):
            team = flatten_dict(i.get('team', None), 'team')
            players = i.get('players', None)
            player_info = list(
                map(lambda x: {**flatten_dict(x.get('player')), **team},
                    players))
            player_statistics = list(
                map(lambda x: flatten_dict(x.get('statistics')[0]), players))
            restult += list(
                map(lambda x, y: {**x, **y}, player_info, player_statistics))
        return restult

    def _get_transformation(self) -> pd.DataFrame:
        players_info = []

        for response in self.file:
            match_id = self.parameters(response).get('fixture')
            jsons_transform = PlayerTransformer.build_json(response)
            players_info += [{**i, 'match_id': match_id} for i in
                             jsons_transform]

        data = pd.DataFrame(players_info)

        return data
