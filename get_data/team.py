from utils.export import export_team_bronze, export_team_silver
from utils.util import config_team_dict
from decouple import config

params = config_team_dict()
export_team_bronze(**params)

export_team_silver(**{'access_key': config('AcessKey'), 'secret_access': config('SecretKey')})
