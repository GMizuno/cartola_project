from decouple import config  # type: ignore
from datetime import date

from utils.export_bronze import export_matches_bronze, export_team_bronze, export_statistics_bronze
from utils.export_silver import export_obt

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
          'date_to': date(2022, 11, 12)}
export_statistics_bronze(**params)

params = {
    'access_key': config('AcessKey'),
    'secret_access': config('SecretKey')
}

export_obt(**params)
