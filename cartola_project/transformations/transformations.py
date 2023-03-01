from abc import abstractmethod, ABC

import pandas as pd

from .util import convert_time, clean_dict_key, convert_date, flatten_dict


class Transformer(ABC):

    @abstractmethod
    def _get_transformation(self):
        pass


class FixturesTransformer(Transformer):

    def __init__(self, file: dict) -> None:
        self.file = file

    def _get_transformation(self) -> pd.DataFrame:

        fixture_json = []
        file = self.file

        for line in file:
            for index, value in enumerate(line.get('response')):
                result = {'partida_id': value.get('fixture').get('id'),
                          'date': convert_time(value.get('fixture').get('date')),
                          'reference_date': convert_date(value.get('fixture').get('date')),
                          'rodada': value.get('league').get('round'),
                          'league_id': value.get('league').get('id'),
                          'id_team_away': value.get('teams').get('away').get('id'),
                          'id_team_home': value.get('teams').get('home').get('id'),
                          'goals_home': value.get('goals').get('home'),
                          'goals_away': value.get('goals').get('away')
                          }
                fixture_json.append(result)
        data = pd.DataFrame([clean_dict_key(i) for i in fixture_json])

        data.rename(columns={'partida_id': 'match_id', 'rodada': 'round'}, inplace=True)
        data.replace(to_replace=r'Regular Season - ', value='', regex=True, inplace=True)
        data.replace(to_replace=r'Group Stage - ', value='', regex=True, inplace=True)

        return data.drop_duplicates()


class TeamsTransformer(Transformer):

    def __init__(self, file: dict) -> None:
        self.file = file

    def _get_transformation(self) -> pd.DataFrame:
        teams_json = []

        for line in self.file:
            teams_json.append({
                'team_id': int(line.get('parameters').get('id')),
                'name': line.get('response')[0].get('team').get('name'),
                'code': line.get('response')[0].get('team').get('code'),
                'country': line.get('response')[0].get('team').get('country'),
                'city': line.get('response')[0].get('venue').get('city'),
                'logo': line.get('response')[0].get('team').get('logo')
            }
            )

        data = pd.DataFrame([clean_dict_key(i) for i in teams_json])

        data_location = data['city'].str.split(',', 1, expand=True)
        data_location.rename(columns={0: 'city', 1: 'state'}, inplace=True)
        data_location['state'] = data_location['state'].fillna(data_location['city'])
        data = data[['team_id', 'name', 'code', 'country', 'logo']]
        data = pd.concat([data, data_location], axis=1)

        return data.drop_duplicates()


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
        data = data.get('response')[0]  # TODO: Rever aqui
        team = flatten_dict(data.get('team', None), 'team')
        players = data.get('players', None)
        player_info = list(
            map(lambda x: {**flatten_dict(x.get('player')), **team}, players))
        player_statistics = list(
            map(lambda x: flatten_dict(x.get('statistics')[0]), players))
        return list(
            map(lambda x, y: {**x, **y}, player_info, player_statistics))

    def _get_transformation(self) -> pd.DataFrame:
        players_info = []

        for response in self.file:
            match_id = self.parameters(response).get('fixture')
            jsons_transform = PlayerTransformer.build_json(response)
            players_info += [{**i, 'match_id': match_id} for i in
                             jsons_transform]

        data = pd.DataFrame(players_info)

        return data
