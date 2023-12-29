import pandas as pd

from cartola_project.models import TeamItem
from cartola_project.transformations.transformations import Transformer


class TeamTransformer(Transformer):
    def __init__(self, file: dict) -> None:
        self.file = file

    def extract_model(self):
        return [TeamItem(**file).model_dump(by_alias=True) for file in self.file]

    @staticmethod
    def extract_field(response: dict) -> dict:
        return {
            'team_id': response.get('team').get('team_id'),
            'name': response.get('team').get('name'),
            'code': response.get('team').get('code'),
            'logo': response.get('team').get('logo'),
            'city': response.get('venue').get('city'),
            'sate': '',
        }

    def transformation(self):
        responses = [response.get('response')[0] for response in self.extract_model()]
        return pd.DataFrame(
            [self.extract_field(response) for response in responses]
        ).drop_duplicates()
