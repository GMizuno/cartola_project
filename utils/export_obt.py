from database.athena import Athena
from cartola.writer import S3Writer

def export_obt(access_key: str, secret_access: str) -> None:  # TODO: Refactor
    athena = Athena(access_key, secret_access)
    data = athena.create_obt()
    S3Writer('bootcamp-gold', access_key, secret_access).upload_fileobj(data, 'obt', 'parquet')

