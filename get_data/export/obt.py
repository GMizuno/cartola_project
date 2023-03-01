import pendulum

from cartola_project import GCSStorage, ParquetWriter
from cartola_project.models import StorageFolder
from get_data.util import create_obt_matches, create_obt_players


def export_obt() -> None:
    gcs = GCSStorage(
        'cartola.json',
        'cartola-360814',
    )

    data_matches = create_obt_matches(gcs)
    data_players = create_obt_players(gcs)

    date = pendulum.now().strftime('%Y-%d-%m')
    file_name = f'{StorageFolder.OBT}/{StorageFolder.OBT_MATCHES}/matches_{date}.parquet'
    ParquetWriter(gcs, 'teste_cartola_gabriel', file_name, data_matches).write()

    file_name = f'{StorageFolder.OBT}/{StorageFolder.OBT_PLAYERS}/players{date}.parquet'
    ParquetWriter(gcs, 'teste_cartola_gabriel', file_name, data_players).write()
