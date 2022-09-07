import glob
import json
from abc import abstractmethod
import datetime
from functools import reduce

from cartola.writer import Writer
from utils.util import convert_time, clean_dict_key, convert_date


class Transformer:

    def __init__(self, filename, type):
        self.filename = filename
        self.type = type

    @property
    def filepath(self):
        if self.type == 'matches':
            return 'matches'
        elif self.type == 'teams':
            return 'teams'
        elif self.type == 'statistics':
            return 'statistics'
        else:
            raise ValueError(f'Type {self.type} does not exist')

    @property
    def read_file(self):
        list_of_files = glob.glob(f'{self.filepath}/*.json')
        file = []
        for list_of_file in list_of_files:
            with open(list_of_file, 'r') as f:
                file.append(json.load(f))

        return reduce(lambda a, b: a + b, file)

    @abstractmethod
    def _get_transformation(self):
        pass

    def save_data(self):
        data = self._get_transformation()
        writer = Writer(self.type)
        writer.write_json_to_parquet(data=data)


class FixturesTransformer(Transformer):

    def __init__(self):
        super().__init__(filename=datetime.datetime.now().year, type='matches')

    def _get_transformation(self):
        file_fixture = self.read_file
        print(type(file_fixture))
        print(len(file_fixture ))

        fixture_json = []

        for line in file_fixture:
            for index, value in enumerate(line.get('response')):
                result = {'partida_id': value.get('fixture').get('id'),
                          'date': convert_time(value.get('fixture').get('date')),
                          'reference_date': convert_date(value.get('fixture').get('date')),
                          'rodada': value.get('league').get('round'),
                          'league_id': value.get('league').get('id'),
                          'id_team_away': value.get('teams').get('away').get('id'),
                          'id_team_home': value.get('teams').get('home').get('id'),
                          }
                fixture_json.append(result)

        return [clean_dict_key(i) for i in fixture_json]


class TeamsTransformer(Transformer):

    def __init__(self):
        super().__init__(filename=datetime.datetime.now().year, type='teams')

    def _get_transformation(self):
        teams_json = []

        for line in self.read_file:
            teams_json.append({
                'team_id': line.get('parameters').get('id'),
                'name': line.get('response')[0].get('team').get('name'),
                'code': line.get('response')[0].get('team').get('code'),
                'country': line.get('response')[0].get('team').get('country'),
                'city': line.get('response')[0].get('venue').get('city'),
                'logo': line.get('response')[0].get('team').get('logo')
            })

        return [clean_dict_key(i) for i in teams_json]


class MatchTransformer(Transformer):

    def __init__(self):
        super().__init__(filename=datetime.datetime.now().strftime("%Y-%m-%d"), type='statistics')

    def _get_transformation(self):
        response = self.read_file

        stats_json = []

        for informations in response:
            for i in informations.get('response'):
                stats_matches = {}
                for j in i.get('statistics'):
                    stats_matches.update({j.get('type'): str(j.get('value'))})
                team_id = i.get('team')['id']
                match_id = informations.get('parameters').get('fixture')
                stats_matches.update({'team_id':  str(team_id), 'match_id':  str(match_id)})
                stats_json.append(stats_matches)

        return [clean_dict_key(i) for i in stats_json]
