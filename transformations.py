import json
from abc import abstractmethod

import pandas as pd

from writer import Writer
from utils.util import convert_time

class Transformer:

    def __init__(self, filename, type):
        self.filename = filename
        self.type = type

    @property
    def filepath(self):
        if self.type == 'partida':
            return 'partida'
        elif self.type == 'team':
            return 'team'
        elif self.type == 'match':
            return 'match'
        else:
            raise ValueError(f'Type {self.type} does not exist')

    @property
    def read_file(self):
        with open(f'{self.filepath}/{self.filename}.json', 'r') as f:
            return json.load(f)

    @abstractmethod
    def _get_transformation(self):
        pass

    def get_data(self):
        data = self._get_transformation()
        writer = Writer(self.type)
        writer.write_json_to_parquet(data)


class FixturesTransformer(Transformer):

    def __init__(self, filename):
        super().__init__(filename, type)
        self.type = 'partida'

    def _get_transformation(self):

        file_fixture = self.read_file[0].get('response')

        fixtures_json = [
            {'partida_id': value.get('fixture').get('id'),
             'date': convert_time(value.get('fixture').get('date')),
             'rodada': value.get('league').get('round'),
             'league_id': value.get('league').get('id'),
             'id_team_away': value.get('teams').get('away').get('id'),
             'id_team_home': value.get('teams').get('home').get('id'),
             } for index, value in enumerate(file_fixture)]

        return fixtures_json

class TeamsTransformer(Transformer):

    def __init__(self, filename):
        super().__init__(filename, type)
        self.type = 'team'

    def _get_transformation(self):

        teams_json = []

        for line in self.read_file:
            teams_json.append({
                'team_id': line .get('parameters').get('id'),
                'name': line.get('response')[0].get('team').get('name'),
                'code': line.get('response')[0].get('team').get('code'),
                'country': line.get('response')[0].get('team').get('country'),
                'city': line.get('response')[0].get('venue').get('city'),
                'logo': line.get('response')[0].get('team').get('logo')
            })

        return teams_json

class MatchTransformer(Transformer):

    def __init__(self, filename):
        super().__init__(filename, type)
        self.type = 'match'

    def _get_transformation(self):
        pass
