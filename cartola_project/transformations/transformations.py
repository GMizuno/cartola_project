from abc import abstractmethod, ABC

import pandas as pd

from cartola_project.models import (Club, Match, Info, StatisticsPlayer, )
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

    def extract_fixture(self) -> list:
        return [file.get('parameters') for file in self.file]

    def extract_teams(self) -> list[dict]:
        team = []
        responses = [(file.get('response'), file.get('parameters')) for file in self.file]
        for response, fixture in responses:
            team.append(response[0] | fixture)
            team.append(response[1] | fixture)

        return team

    def to_dataframe(self) -> pd.DataFrame:
        list_model_dict = [
            flatten_dict(model.to_dict())
            for model in
            self.extract_model()
        ]
        return pd.DataFrame(list_model_dict).drop_duplicates()

    def extract_model(self):
        statistics = []
        for team in self.extract_teams():
            info = Info.from_dict(team)
            team_model = info.team
            fixture_model = info.fixture
            for player in info.players:
                player_model = player.player
                statistics_model = player.statistics[0]
                statistics.append(StatisticsPlayer(team_model, fixture_model,
                                                   player_model,
                                                   statistics_model))
        return statistics

    def _get_transformation(self):
        data = self.to_dataframe()
        data.drop(['team_logo', 'team_update', 'player_photo', ], axis=1,
                  inplace=True)
        data.fillna(0, inplace=True)
        return data
