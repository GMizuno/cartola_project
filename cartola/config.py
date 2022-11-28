import datetime

from decouple import config


def config_matches_dict(league_id: str,
                        season_year: str,
                        api_host_key: str = 'API_HOST_KEY',
                        api_secert_key: str = 'API_SECERT_KEY',
                        access_key: str = 'AcessKey',
                        secret_access: str = 'SecretKey') -> dict:
    return {
            'api_host_key': config(api_host_key),
            'api_secert_key': config(api_secert_key),
            'league_id': league_id,
            'season_year': season_year,
            'access_key': config(access_key),
            'secret_access': config(secret_access)
    }


def config_team_dict(api_host_key: str = 'API_HOST_KEY',
                     api_secert_key: str = 'API_SECERT_KEY',
                     access_key: str = 'AcessKey',
                     secret_access: str = 'SecretKey') -> dict:
    return {
            'api_host_key': config(api_host_key),
            'api_secert_key': config(api_secert_key),
            'access_key': config(access_key),
            'secret_access': config(secret_access)
    }


def config_statistics_dict(date_from: datetime.date,
                           date_to: datetime.date,
                           api_host_key: str = 'API_HOST_KEY',
                           api_secert_key: str = 'API_SECERT_KEY',
                           access_key: str = 'AcessKey',
                           secret_access: str = 'SecretKey') -> dict:
    return {
            'api_host_key': config(api_host_key),
            'api_secert_key': config(api_secert_key),
            'date_from': date_from,
            'date_to': date_to,
            'access_key': config(access_key),
            'secret_access': config(secret_access)
    }


def config_obt_dict(access_key: str = 'AcessKey',
                    secret_access: str = 'SecretKey') -> dict:
    return {
            'access_key': config(access_key),
            'secret_access': config(secret_access)
    }
