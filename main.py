from decouple import config  # type: ignore
from datetime import date

from cartola.api import Teams, Matches
from cartola.athena import Athena
from cartola.writer import S3Writer
from cartola.transformations import FixturesTransformer, TeamsTransformer, MatchTransformer
from utils.export_bronze import export_matches_bronze, export_team_bronze, export_statistics_bronze

## Partidas - Brasileirao serie A
params = {'api_host_key': config('API_HOST_KEY'),
          'api_secert_key': config('API_SECERT_KEY'),
          'league_id': '71',
          'season_year': '2022',
          'access_key': config('AcessKey'),
          'secret_access': config('SecretKey')}
export_matches_bronze(**params)

params = {'api_host_key': config('API_HOST_KEY'),
          'api_secert_key': config('API_SECERT_KEY'),
          'league_id': '39',
          'season_year': '2022',
          'access_key': config('AcessKey'),
          'secret_access': config('SecretKey')}
export_matches_bronze(**params)

params = {'api_host_key': config('API_HOST_KEY'),
          'api_secert_key': config('API_SECERT_KEY'),
          'access_key': config('AcessKey'),
          'secret_access': config('SecretKey')}
export_team_bronze(**params)

params = {'api_host_key': config('API_HOST_KEY'),
          'api_secert_key': config('API_SECERT_KEY'),
          'access_key': config('AcessKey'),
          'secret_access': config('SecretKey'),
          'date_from': date(2022, 10, 1),
          'date_to':date(2022, 10, 6)}
export_statistics_bronze(**params)