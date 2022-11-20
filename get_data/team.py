from utils.export_bronze import export_team_bronze
from utils.util import config_team_dict

params = config_team_dict()
export_team_bronze(**params)