import json
from abc import abstractmethod
import datetime
from functools import reduce
import tempfile
import pandas as pd
from io import BytesIO
import boto3

from utils.util import convert_time, clean_dict_key, convert_date


class Transformer:

    def __init__(self, bucket: str, s3_folder: str, access_key, secret_access):
        self.bucket = bucket
        self.s3_folder = s3_folder
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_access
        )

    def get_file_name(self):
        return f'{self.s3_folder}_{datetime.datetime.now().strftime("%Y-%d-%m_%H-%M-%S")}'

    def get_matching_s3_keys(self, suffix: str = '', **kwargs):

        while True:
            resp = self.s3.list_objects_v2(Prefix=self.s3_folder, Bucket=self.bucket)
            for obj in resp['Contents']:
                key = obj['Key']
                if key.endswith(suffix):
                    yield key
            try:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            except KeyError:
                break

    def read_file_json(self, **kwargs):
        result = [file for file in self.get_matching_s3_keys('json', **kwargs)]
        file = []
        for x in result:
            with tempfile.TemporaryFile() as data:
                self.s3.download_fileobj(self.bucket, x, data)
                data.seek(0)
                file.append(json.loads(data.read().decode('utf-8')))

        return reduce(lambda a, b: a + b, file)

    def read_file_parquet(self, **kwargs):
        files = [file for file in self.get_matching_s3_keys('parquet', **kwargs)]
        file_bytes = []

        for file in files:
            buffer = BytesIO()
            self.s3.download_fileobj('bootcamp-silver', file, buffer)
            file_bytes.append(buffer)

        return pd.concat([pd.read_parquet(bs) for bs in file_bytes])

    def read_file(self, suffix, **kwargs):

        if suffix == 'json':
            return self.read_file_json(**kwargs)
        elif suffix == 'parquet':
            return self.read_file_parquet(**kwargs)
        else:
            raise ValueError('Extension does not supported')

    @abstractmethod
    def _get_transformation(self):
        pass

    @abstractmethod
    def _get_transformation_gold(self):
        pass


class FixturesTransformer(Transformer):

    def __init__(self, access_key, secret_access, bucket='bootcamp-bronze', s3_folder='matches'):
        super().__init__(bucket, s3_folder, access_key, secret_access)
        self.acess_key = access_key
        self.secret_key = secret_access
        self.storage_option = {'key': access_key,
                               'secret': secret_access}

    def _get_transformation(self):

        fixture_json = []

        for line in self.read_file(suffix='json'):
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

        return pd.DataFrame([clean_dict_key(i) for i in fixture_json])

    def _get_transformation_gold(self):
        data = self.read_file(suffix='parquet')

        data.rename(columns={'partida_id': 'match_id', 'rodada': 'round'}, inplace=True)
        data.replace(to_replace=r'Regular Season - ', value='', regex=True, inplace=True)

        return data.drop_duplicates()


class TeamsTransformer(Transformer):

    def __init__(self, access_key, secret_access, bucket='bootcamp-bronze', s3_folder='teams'):
        super().__init__(bucket, s3_folder, access_key, secret_access)
        self.acess_key = access_key
        self.secret_key = secret_access
        self.storage_option = {'key': access_key,
                               'secret': secret_access}

    def _get_transformation(self):
        teams_json = []

        for line in self.read_file(suffix='json'):
            teams_json.append({
                'team_id': line.get('parameters').get('id'),
                'name': line.get('response')[0].get('team').get('name'),
                'code': line.get('response')[0].get('team').get('code'),
                'country': line.get('response')[0].get('team').get('country'),
                'city': line.get('response')[0].get('venue').get('city'),
                'logo': line.get('response')[0].get('team').get('logo')
            })

        return pd.DataFrame([clean_dict_key(i) for i in teams_json])

    def _get_transformation_gold(self):
        data = self.read_file(suffix='parquet')

        data_location = data['city'].str.split(',', 1, expand=True)
        data_location.rename(columns={0: 'city', 1: 'state'}, inplace=True)
        data_location['state'] = data_location['state'].fillna(data_location['city'])

        data = data[['team_id', 'name', 'code', 'country', 'logo']]

        data = pd.concat([data, data_location], axis=1)

        return data.drop_duplicates()


class MatchTransformer(Transformer):

    def __init__(self, access_key, secret_access, bucket='bootcamp-bronze', s3_folder='statistics'):
        super().__init__(bucket, s3_folder, access_key, secret_access)
        self.acess_key = access_key
        self.secret_key = secret_access
        self.storage_option = {'key': access_key,
                               'secret': secret_access}

    def _get_transformation(self):

        stats_json = []

        for informations in self.read_file(suffix='json'):
            for i in informations.get('response'):
                stats_matches = {}
                for j in i.get('statistics'):
                    stats_matches.update({j.get('type'): str(j.get('value'))})
                team_id = i.get('team')['id']
                match_id = informations.get('parameters').get('fixture')
                stats_matches.update({'team_id': str(team_id), 'match_id': str(match_id)})
                stats_json.append(stats_matches)

        return pd.DataFrame([clean_dict_key(i) for i in stats_json])

    def _get_transformation_gold(self):
        data = self.read_file(suffix='parquet')

        data.replace('None', 0, inplace=True)
        data.replace(to_replace=r'%', value='', regex=True, inplace=True)
        data = data.astype(int)
        data['Passes_percentage'] = data['Passes_percentage'].div(100)
        data['Ball_Possession'] = data['Ball_Possession'].div(100)

        return data.drop_duplicates()
