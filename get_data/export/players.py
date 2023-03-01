from datetime import date

import pendulum

from cartola_project import Players, GCSStorage, JsonWriter, ParquetWriter
from cartola_project.models import StorageFolder, Bucket
from cartola_project.transformations import PlayerTransformer
from get_data.util import filter_by_date


def export_player_bronze(api_host_key: str,
                         api_secert_key: str,
                         league_id: str,
                         season_year: str,
                         date_from: date,
                         date_to: date,
                         ) -> list:
    partidas = Players(api_host_key, api_secert_key)
    gcs = GCSStorage('cartola.json', 'cartola-360814')
    date = pendulum.now().strftime("%Y-%d-%m_%H:%M:%S")

    matches_id = filter_by_date(gcs, league_id, season_year, date_from,
                                date_to)[0:3]
    data = partidas.get_data(match_id=matches_id)

    file_name = f"{StorageFolder.PLAYERS}/{Bucket.BRONZE}/league={league_id}/season={season_year}/{date}.json"
    JsonWriter(gcs, "teste_cartola_gabriel", file_name, data).write()

    return data


def export_player_silver(
        file: dict | list[dict], league_id: str, season_year: str
) -> None:
    data = PlayerTransformer(file)._get_transformation()

    gcs = GCSStorage("cartola.json", "cartola-360814")
    date = pendulum.now().strftime("%Y-%d-%m_%H:%M:%S")
    file_name = f"{StorageFolder.PLAYERS}/{Bucket.SILVER}/league={league_id}/season={season_year}/{date}.parquet"
    print(file_name)
    ParquetWriter(gcs, "teste_cartola_gabriel", file_name, data).write()
