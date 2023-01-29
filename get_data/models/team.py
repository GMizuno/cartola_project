from decouple import config

from get_data.export import export_team_bronze, export_team_silver


def config_team_dict(api_host_key: str = 'API_HOST_KEY',
                     api_secert_key: str = 'API_SECERT_KEY', ) -> dict:
    return {
        'api_host_key': config(api_host_key),
        'api_secert_key': config(api_secert_key),
    }


params = config_team_dict()
export_team_bronze(**params)

export_team_silver(**{'access_key': config('AcessKey'), 'secret_access': config('SecretKey')})
