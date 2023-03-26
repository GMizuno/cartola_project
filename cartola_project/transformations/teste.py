import json
from abc import abstractmethod, ABC

import pandas as pd

from cartola_project.models.statistic import Info, StatisticsPlayer
from cartola_project.transformations.util import flatten_dict


class Transformer(ABC):

    @abstractmethod
    def _get_transformation(self):
        pass


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

