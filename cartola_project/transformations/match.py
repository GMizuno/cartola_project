import pandas as pd

from cartola_project.models import MatchItem
from cartola_project.transformations.transformations import Transformer


class MatchTransformer(Transformer):
    def __init__(self, file: dict) -> None:
        self.file = file[0]

    def extract_model(self):
        return MatchItem(**self.file).model_dump(by_alias=True)

    @staticmethod
    def extract_field(response: dict) -> dict:
        return {
            'partida_id': response.get('fixture').get('id'),
            'date': response.get('fixture').get('date'),
            'reference_date': response.get('fixture').get('reference_date'),
            'rodada': response.get('league').get('rodada'),
            'league_id': response.get('league').get('league_id'),
            'id_team_away': response.get('teams').get('info').get('id_team_away'),
            'id_team_home': response.get('teams').get('info').get('id_team_home'),
            'goals_home': response.get('goals').get('goals_home'),
            'goals_away': response.get('goals').get('goals_away'),
            'winner_home': response.get('teams').get('info').get('winner_home'),
            'winner_away': response.get('teams').get('info').get('winner_away'),
        }

    def transformation(self):
        responses = self.extract_model().get('response')
        return pd.DataFrame(
            [self.extract_field(response) for response in responses]
        ).drop_duplicates()
