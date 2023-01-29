import pendulum

from cartola_project import Fixtures, GCSStorage, JsonWriter, ParquetWriter
from cartola_project.models import Bucket, StorageFolder
from cartola_project.transformations import FixturesTransformer


def export_matches_bronze(api_host_key: str,
                          api_secert_key: str,
                          league_id: str,
                          season_year: str) -> list[dict]:
    partidas = Fixtures(api_host_key, api_secert_key)
    data = partidas.get_data(league_id=league_id, season_year=season_year)

    gcs = GCSStorage('cartola.json', 'cartola-360814')
    date = pendulum.now().strftime('%Y-%d-%m_%H:%M:%S')
    file_name = f'{StorageFolder.MATCHES}/{Bucket.BRONZE}/{league_id}_{season_year}_{date}.json'
    JsonWriter(gcs, 'teste_cartola_gabriel', file_name, data).write()

    return data


def export_matches_silver(file: dict | list[dict], league_id: str, season_year: str) -> None:
    data = FixturesTransformer(file)._get_transformation()

    gcs = GCSStorage('cartola.json', 'cartola-360814')
    date = pendulum.now().strftime('%Y-%d-%m_%H:%M:%S')
    file_name = f'{StorageFolder.MATCHES}/{Bucket.SILVER}/{league_id}_{season_year}_{date}.parquet'
    ParquetWriter(gcs, 'teste_cartola_gabriel', file_name, data).write()
#
#
# def export_team_bronze(api_host_key: str, api_secert_key: str, access_key: str,
#                        secret_access: str) -> None:
#     times = Teams(api_host_key, api_secert_key)
#     ids = get_all_ids(access_key, secret_access)
#     data = times.get_data(team_id=ids)
#
#     S3WriterJson(Bucket.BRONZE, access_key, secret_access). \
#         upload_fileobj(data, StorageFolder.TEAMS)
#
#
# def export_team_silver(access_key: str, secret_access: str) -> None:
#     data_partidas = TeamsTransformer(access_key=access_key,
#                                      secret_access=secret_access
#                                      )._get_transformation()
#
#     S3WriterParquet(Bucket.SILVER, access_key, secret_access). \
#         upload_fileobj(data_partidas, StorageFolder.TEAMS)
#
#
# def export_statistics_bronze(api_host_key: str, api_secert_key: str, date_from: date,
#                              date_to: date, access_key: str,
#                              secret_access: str) -> None:
#     statistics = Matches(api_host_key, api_secert_key)
#     ids = filter_by_date(access_key, secret_access, date_from, date_to)
#     data = statistics.get_data(match_id=ids)
#
#     gcs = GCSStorage('cartola.json', 'cartola-360814')
#     S3WriterJson(Bucket.BRONZE, access_key, secret_access). \
#         upload_fileobj(data, StorageFolder.STATISTICS)
#
#
# def export_statistics_silver(access_key: str, secret_access: str) -> None:
#     data_partidas = MatchTransformer(access_key=access_key,
#                                      secret_access=secret_access
#                                      )._get_transformation()
#
#     S3WriterParquet(Bucket.SILVER, access_key, secret_access). \
#         upload_fileobj(data_partidas, StorageFolder.STATISTICS)
#
#
# def export_obt(access_key: str, secret_access: str) -> None:
#     data = create_obt(access_key, secret_access)
#     S3WriterParquet(Bucket.GOLD, access_key, secret_access).upload_fileobj(data, StorageFolder.OBT)
