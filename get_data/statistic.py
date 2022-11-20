from datetime import date

from utils.export_bronze import export_statistics_bronze
from utils.export_silver import export_obt
from utils.util import config_statistics_dict, config_obt_dict

params = config_statistics_dict(
        date(2022, 10, 1),
        date(2022, 11, 12)
)
export_statistics_bronze(**params)

params = config_obt_dict()

export_obt(**params)
