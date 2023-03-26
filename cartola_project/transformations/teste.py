import json
from abc import abstractmethod, ABC

import pandas as pd

from cartola_project.models import Club
from cartola_project.transformations.util import clean_dict_key


class Transformer(ABC):

    @abstractmethod
    def _get_transformation(self):
        pass


class TeamsTransformer(Transformer):

    def __init__(self, file: dict) -> None:
        self.file = file

    def extract_model(self):
        response = map(lambda x: x.get('response'), self.file)
        return map(lambda x: Club.from_dict(x[0]), response)

    def to_dataframe(self) -> pd.DataFrame:
        teams_json = self.extract_model()
        data = pd.DataFrame([clean_dict_key(i) for i in teams_json])

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

        return self.to_dataframe()
