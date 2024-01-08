from itertools import chain

import pandas as pd
from flatten_json import flatten

from cartola_project.models import PlayerItem
from cartola_project.transformations.transformations import Transformer


class PlayerTransformer(Transformer):
    def __init__(self, file: list[dict]) -> None:
        self.file = file

    def extract_model(self):
        return [PlayerItem(**file).model_dump(by_alias=True) for file in self.file]

    @staticmethod
    def extract_team_field(response: dict) -> dict:
        return response.get('team')

    @staticmethod
    def extract_match_id(response: dict) -> dict:
        return response.get('match')

    @staticmethod
    def extract_player_field(response: dict) -> list[dict]:
        players = response.get('players')
        return [player.get('player') | flatten(player.get('statistics')[0]) for player in players]

    def extract_field(self, response: dict) -> list[dict]:
        players = self.extract_player_field(response)
        team = self.extract_team_field(response)
        match = self.extract_match_id(response)
        return [match | team | player for player in players]

    def transformation(self):
        model = self.extract_model()
        responses = list(chain.from_iterable([[campo | {'match': i.get('parameters')} for campo in i.get('response')] for i in model]))
        return pd.DataFrame(list(chain.from_iterable([self.extract_field(i) for i in responses])))
