from utils.export_bronze import export_matches_bronze
from utils.util import config_matches_dict

params = [config_matches_dict('39', '2022'), config_matches_dict('71', '2022'), config_matches_dict('1', '2018')]
for param in params:
    export_matches_bronze(**param)
