from get_data.export import export_team_bronze, export_team_silver
from cartola_project import config_team_dict
from decouple import config

params = config_team_dict()
export_team_bronze(**params)

export_team_silver(**{'access_key': config('AcessKey'), 'secret_access': config('SecretKey')})
