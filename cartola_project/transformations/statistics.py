from itertools import chain

import pandas as pd

from cartola_project.models import StatisticsItem
from cartola_project.transformations.transformations import Transformer


class StatisticsTransformer(Transformer):
    def __init__(self, file: list[dict]) -> None:
        self.file = file

    def extract_model(self):
        return [StatisticsItem(**file).model_dump(by_alias=True) for file in self.file]

    @staticmethod
    def extract_team_field(response: dict) -> dict:
        return response.get('team')

    @staticmethod
    def extract_match_id(response: dict) -> dict:
        return response.get('match')

    @staticmethod
    def extract_stats_field(response: dict) -> dict:
        return response.get('stats')

    def extract_field(self, response: dict) -> dict:
        stats = self.extract_stats_field(response)
        team = self.extract_team_field(response)
        match = self.extract_match_id(response)
        return match | team | stats

    def transformation(self):
        model = self.extract_model()
        responses = list(chain.from_iterable([[campo | {'match': i.get('parameters')} for campo in i.get('response')] for i in model]))
        return pd.DataFrame([self.extract_field(i) for i in responses])
