from datetime import date

import pendulum

from cartola_project import Fixtures, Teams, Matches, GCSStorage, JsonWriter, ParquetWriter
from cartola_project.models import Bucket, StorageFolder
from cartola_project.transformations import FixturesTransformer, TeamsTransformer, MatchTransformer
from get_data.process import get_all_ids, filter_by_date, create_obt


def export_matches_bronze(api_host_key: str,
                          api_secert_key: str,
                          league_id: str,
                          season_year: str) -> list[dict]:
    partidas = Fixtures(api_host_key, api_secert_key)
    data = partidas.get_data(league_id=league_id, season_year=season_year)

    gcs = GCSStorage('cartola.json', 'cartola-360814')
    date = pendulum.now().strftime('%Y-%d-%m_%H:%M:%S')
    file_name = f'{StorageFolder.MATCHES}/{Bucket.BRONZE}/league={league_id}/season={season_year}/{date}.json'
    JsonWriter(gcs, 'teste_cartola_gabriel', file_name, data).write()

    return data


def export_matches_silver(file: dict | list[dict], league_id: str, season_year: str) -> None:
    data = FixturesTransformer(file)._get_transformation()

    gcs = GCSStorage('cartola.json', 'cartola-360814')
    date = pendulum.now().strftime('%Y-%d-%m_%H:%M:%S')
    file_name = f'{StorageFolder.MATCHES}/{Bucket.SILVER}/league={league_id}/season={season_year}/{date}.parquet'
    ParquetWriter(gcs, 'teste_cartola_gabriel', file_name, data).write()


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


def export_team_silver(file: dict | list[dict], league_id: str, season_year: str) -> None:
    data = TeamsTransformer(file)._get_transformation()

    gcs = GCSStorage('cartola.json', 'cartola-360814')
    date = pendulum.now().strftime('%Y-%d-%m_%H:%M:%S')
    file_name = f'{StorageFolder.TEAMS}/{Bucket.SILVER}/league={league_id}/season={season_year}/{date}.parquet'
    ParquetWriter(gcs, 'teste_cartola_gabriel', file_name, data).write()


def export_statistics_bronze(api_host_key: str,
                             api_secert_key: str,
                             league_id: str,
                             season_year: str,
                             date_from: date,
                             date_to: date,
                             ) -> None:
    statistics = Matches(api_host_key, api_secert_key)
    gcs = GCSStorage('cartola.json', 'cartola-360814')

    matches_id = filter_by_date(gcs, league_id, season_year, date_from, date_to)
    data = statistics.get_data(match_id=matches_id)

    date = pendulum.now().strftime('%Y-%d-%m_%H:%M:%S')
    file_name = f'{StorageFolder.STATISTICS}/{Bucket.BRONZE}/league={league_id}/season={season_year}/{date}.json'
    JsonWriter(gcs, 'teste_cartola_gabriel', file_name, data).write()

    return data


def export_statistics_silver(file: dict | list[dict], league_id: str, season_year: str) -> None:
    data = MatchTransformer(file)._get_transformation()

    gcs = GCSStorage('cartola.json', 'cartola-360814')
    date = pendulum.now().strftime('%Y-%d-%m_%H:%M:%S')
    file_name = f'{StorageFolder.STATISTICS}/{Bucket.SILVER}/league={league_id}/season={season_year}/{date}.parquet'
    ParquetWriter(gcs, 'teste_cartola_gabriel', file_name, data).write()


def export_obt() -> None:
    gcs = GCSStorage('cartola.json', 'cartola-360814')
    data = create_obt(gcs)

    date = pendulum.now().strftime('%Y-%d-%m')
    file_name = f'{StorageFolder.OBT}/{date}.parquet'
    ParquetWriter(gcs, 'teste_cartola_gabriel', file_name, data).write()
