import pendulum

from cartola_project import GCSStorage, ParquetWriter
from cartola_project.models import StorageFolder
from get_data.process import create_obt


def export_obt() -> None:
    gcs = GCSStorage('cartola.json', 'cartola-360814')
    data = create_obt(gcs)

    date = pendulum.now().strftime('%Y-%d-%m')
    file_name = f'{StorageFolder.OBT}/{date}.parquet'
    ParquetWriter(gcs, 'teste_cartola_gabriel', file_name, data).write()
