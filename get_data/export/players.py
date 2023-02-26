import pendulum

from cartola_project import Players, GCSStorage, JsonWriter, ParquetWriter
from cartola_project.models import StorageFolder, Bucket
from cartola_project.transformations import PlayerTransformer


def export_matches_bronze(
        api_host_key: str, api_secert_key: str, league_id: str, season_year: str
) -> list[dict]:
    partidas = Players(api_host_key, api_secert_key)
    data = partidas.get_data(league_id=league_id, season_year=season_year)

    gcs = GCSStorage("cartola.json", "cartola-360814")
    date = pendulum.now().strftime("%Y-%d-%m_%H:%M:%S")
    file_name = f"{StorageFolder.MATCHES}/{Bucket.BRONZE}/league={league_id}/season={season_year}/{date}.json"
    JsonWriter(gcs, "teste_cartola_gabriel", file_name, data).write()

    return data


def export_matches_silver(
        file: dict | list[dict], league_id: str, season_year: str
) -> None:
    data = PlayerTransformer(file)._get_transformation()

    gcs = GCSStorage("cartola.json", "cartola-360814")
    date = pendulum.now().strftime("%Y-%d-%m_%H:%M:%S")
    file_name = f"{StorageFolder.MATCHES}/{Bucket.SILVER}/league={league_id}/season={season_year}/{date}.parquet"
    ParquetWriter(gcs, "teste_cartola_gabriel", file_name, data).write()
