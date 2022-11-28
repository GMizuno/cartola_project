from get_data.export import export_obt
from decouple import config

export_obt(**{'access_key': config('AcessKey'), 'secret_access': config('SecretKey')})
