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
        return {
            'teams_id': response.get('team').get('teams_id'),
        }

    @staticmethod
    def extract_player_field(response: dict) -> list[dict]:
        players = response.get('players')
        return [player.get('player') | flatten(player.get('statistics')[0]) for player in players]

    def extract_field(self, response: dict) -> list[dict]:
        players = self.extract_player_field(response)
        team = self.extract_team_field(response)
        return [team | player for player in players]

    def transformation(self):
        responses = list(
            chain.from_iterable([response.get('response') for response in self.extract_model()])
        )
        data = chain.from_iterable([self.extract_field(response) for response in responses])
        return pd.DataFrame(data)
