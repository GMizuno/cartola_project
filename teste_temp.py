from tempfile import NamedTemporaryFile
import json
import boto3
from decouple import config

from cartola.transformations import FixturesTransformer

teste = FixturesTransformer(access_key=config('AcessKey'),
                            secret_access=config('SecretKey'))
files = teste.read_file_json()
teste = reduce(lambda a, b: a + b, files)