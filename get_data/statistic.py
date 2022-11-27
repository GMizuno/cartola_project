from datetime import date

from utils.export import export_statistics_bronze, export_statistics_silver
from utils.util import config_statistics_dict
from decouple import config

params = config_statistics_dict(
        date(2022, 10, 1),
        date(2022, 10, 12)
)
export_statistics_bronze(**params)

export_statistics_silver(**{'access_key': config('AcessKey'), 'secret_access': config('SecretKey')})
