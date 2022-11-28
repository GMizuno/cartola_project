from get_data.export import export_matches_bronze, export_matches_silver
from cartola import config_matches_dict
from decouple import config

params = [config_matches_dict('39', '2022'), config_matches_dict('71', '2022'), config_matches_dict('1', '2022')]
for param in params:
    export_matches_bronze(**param)

params = {'access_key': config('AcessKey'), 'secret_access': config('SecretKey')}
export_matches_silver(**params)

