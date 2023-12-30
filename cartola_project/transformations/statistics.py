from itertools import chain

import pandas as pd

from cartola_project.models import StatisticsItem
from cartola_project.transformations.transformations import Transformer


class TeamTransformer(Transformer):
    def __init__(self, file: list[dict]) -> None:
        self.file = file

    def extract_model(self):
        return [StatisticsItem(**file).model_dump(by_alias=True) for file in self.file]

    @staticmethod
    def extract_team_field(response: dict) -> dict:
        return {
            'teams_id': response.get('team').get('team_id'),
        }

    @staticmethod
    def extract_stats_field(response: dict) -> dict:
        return response.get('stats')

    def extract_field(self, response: dict) -> dict:
        stats = self.extract_stats_field(response)
        team = self.extract_team_field(response)
        return team | stats

    def transformation(self):
        responses = list(
            chain.from_iterable([response.get('response') for response in self.extract_model()])
        )

        return pd.DataFrame([self.extract_field(response) for response in responses])
