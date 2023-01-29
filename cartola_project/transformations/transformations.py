from abc import abstractmethod, ABC

import pandas as pd

from cartola_project.models import Bucket, StorageFolder
from .util import convert_time, clean_dict_key, convert_date


class Transformer(ABC):

    @abstractmethod
    def _get_transformation(self):
        pass


class FixturesTransformer(Transformer):

    def __init__(self, file: dict) -> None:
        self.file = file

    def _get_transformation(self) -> pd.DataFrame:

        fixture_json = []
        file = self.file

        for line in file:
            for index, value in enumerate(line.get('response')):
                result = {'partida_id': value.get('fixture').get('id'),
                          'date': convert_time(value.get('fixture').get('date')),
                          'reference_date': convert_date(value.get('fixture').get('date')),
                          'rodada': value.get('league').get('round'),
                          'league_id': value.get('league').get('id'),
                          'id_team_away': value.get('teams').get('away').get('id'),
                          'id_team_home': value.get('teams').get('home').get('id'),
                          'goals_home': value.get('goals').get('home'),
                          'goals_away': value.get('goals').get('away')
                          }
                fixture_json.append(result)
        data = pd.DataFrame([clean_dict_key(i) for i in fixture_json])

        data.rename(columns={'partida_id': 'match_id', 'rodada': 'round'}, inplace=True)
        data.replace(to_replace=r'Regular Season - ', value='', regex=True, inplace=True)
        data.replace(to_replace=r'Group Stage - ', value='', regex=True, inplace=True)

        return data.drop_duplicates()


class TeamsTransformer(Transformer):

    def __init__(self, bucket: Bucket, storage_folder: StorageFolder, cloud_storage) -> None:
        self.bucket = bucket
        self.storage_folder = storage_folder
        self.cloud_storage = cloud_storage

    def _get_transformation(self) -> pd.DataFrame:
        teams_json = []

        for line in self.read.read_file():
            teams_json.append({
                'team_id': int(line.get('parameters').get('id')),
                'name': line.get('response')[0].get('team').get('name'),
                'code': line.get('response')[0].get('team').get('code'),
                'country': line.get('response')[0].get('team').get('country'),
                    'city': line.get('response')[0].get('venue').get('city'),
                    'logo': line.get('response')[0].get('team').get('logo')
            }
            )

        data = pd.DataFrame([clean_dict_key(i) for i in teams_json])

        data_location = data['city'].str.split(',', 1, expand=True)
        data_location.rename(columns={0: 'city', 1: 'state'}, inplace=True)
        data_location['state'] = data_location['state'].fillna(data_location['city'])
        data = data[['team_id', 'name', 'code', 'country', 'logo']]
        data = pd.concat([data, data_location], axis=1)

        return data.drop_duplicates()


class MatchTransformer(Transformer):

    def __init__(self, bucket: Bucket, storage_folder: StorageFolder, cloud_storage) -> None:
        self.bucket = bucket
        self.storage_folder = storage_folder
        self.cloud_storage = cloud_storage

    def _get_transformation(self) -> pd.DataFrame:

        stats_json = []

        for informations in self.read.read_file():
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
