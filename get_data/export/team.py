import pendulum

from cartola_project import GCSStorage, Teams, JsonWriter, ParquetWriter
from cartola_project.models import StorageFolder, Bucket
from cartola_project.transformations import TeamsTransformer
from get_data.process import get_all_ids


def export_team_bronze(api_host_key: str,
                       api_secert_key: str,
                       league_id: str,
                       season_year: str) -> list[dict]:
    gcs = GCSStorage('cartola.json', 'cartola-360814')
    times = Teams(api_host_key, api_secert_key)

    ids = get_all_ids(gcs, league_id, season_year)
    data = times.get_data(team_id=ids)

    date = pendulum.now().strftime('%Y-%d-%m_%H:%M:%S')
    file_name = f'{StorageFolder.TEAMS}/{Bucket.BRONZE}/league={league_id}/season={season_year}/{date}.json'
    JsonWriter(gcs, 'teste_cartola_gabriel', file_name, data).write()

    return data


def export_team_silver(file: dict | list[dict], league_id: str,
                       season_year: str) -> None:
    data = TeamsTransformer(file)._get_transformation()

    gcs = GCSStorage('cartola.json', 'cartola-360814')
    date = pendulum.now().strftime('%Y-%d-%m_%H:%M:%S')
    file_name = f'{StorageFolder.TEAMS}/{Bucket.SILVER}/league={league_id}/season={season_year}/{date}.parquet'
    ParquetWriter(gcs, 'teste_cartola_gabriel', file_name, data).write()
