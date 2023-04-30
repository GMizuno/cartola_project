from abc import ABC, abstractmethod

import pandas as pd

from cartola_project.models import Club, Info, Match, StatisticsPlayer, TeamStatistics
from cartola_project.transformations.util import (
    clean_dict_key,
    convert_date,
    convert_time,
    flatten_dict,
    merge_dict,
    tranform_stats,
    unlist,
)


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

        data.rename(columns={'partida_id': 'match_id', 'rodada': 'round'}, inplace=True)
        data.replace(to_replace=r'Regular Season - ', value='', regex=True, inplace=True)
        data.replace(to_replace=r'Group Stage - ', value='', regex=True, inplace=True)

        return data.drop_duplicates()

    def _get_transformation(self) -> pd.DataFrame:
        fixture_json = []

        for fixture in self.extract_model():
            result = {
                'partida_id': fixture.fixture.id,
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
        data_location['state'] = data_location['state'].fillna(data_location['city'])
        data = data[['team_id', 'name', 'code', 'country', 'logo']]
        data = pd.concat([data, data_location], axis=1)

        return data.drop_duplicates()

    def _get_transformation(self) -> pd.DataFrame:
        teams_json = []

        for club in self.extract_model():
            teams_json.append(
                {
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

    def to_dataframe(self, list_model: list):
        list_model_flat = [flatten_dict(model.to_dict()) for model in list_model]

        data = pd.DataFrame([clean_dict_key(i) for i in list_model_flat])

        return data.drop_duplicates()

    def extract_parameters(self, dict_model: dict) -> dict:
        return dict_model.get('parameters')

    def extract_respose(self, dict_model) -> list:
        return dict_model.get('response')

    def extract_team_statistics(self, dict_model: dict) -> list[TeamStatistics]:
        response_teams = self.extract_respose(dict_model)
        team_statistics_list = []
        for response_team in response_teams:
            stats = response_team.get('statistics')
            team = response_team.get('team')
            new_stat = merge_dict(list(map(tranform_stats, stats)))

            new_stat = {'statistics': new_stat}
            team = {'team': team}
            statistics = new_stat | team | self.extract_parameters(dict_model)
            team_statistics_list.append(TeamStatistics.from_dict(statistics))

        return team_statistics_list

    def _get_transformation(self) -> pd.DataFrame:
        stats_json = []

        for statistics in self.file:
            stats_json.append(self.extract_team_statistics(statistics))

        data = self.to_dataframe(unlist(stats_json))
        data.replace(to_replace=r'%', value='', regex=True, inplace=True)
        data.fillna(0, inplace=True)
        data = data.astype(
            {
                'statistics_Passesperc': 'int32',
                'statistics_BallPossession': 'int32',
            }
        )
        data['statistics_Passesperc'] = data['statistics_Passesperc'].div(100)
        data['statistics_BallPossession'] = data['statistics_BallPossession'].div(100)

        data.drop(
            [
                'team_logo',
                'team_name',
            ],
            axis=1,
            inplace=True,
        )

        return data


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
        list_model_flat = [flatten_dict(model.to_dict()) for model in self.extract_model()]
        return pd.DataFrame(list_model_flat).drop_duplicates()

    def extract_model(self):
        statistics = []
        for team in self.extract_teams():
            info = Info.from_dict(team)
            team_model = info.team
            fixture_model = info.fixture
            for player in info.players:
                player_model = player.player
                statistics_model = player.statistics[0]
                statistics.append(
                    StatisticsPlayer(
                        team_model,
                        fixture_model,
                        player_model,
                        statistics_model,
                    )
                )
        return statistics

    def _get_transformation(self):
        data = self.to_dataframe()
        data.drop(
            [
                'team_logo',
                'team_update',
                'player_photo',
            ],
            axis=1,
            inplace=True,
        )
        data.fillna(0, inplace=True)
        return data
