from datetime import date

from cartola import Fixtures, Teams, Matches, S3WriterJson, S3WriterParquet
from cartola.transformations import FixturesTransformer, TeamsTransformer, MatchTransformer
from cartola.models import Bucket, Storage
from get_data.process import create_obt, filter_by_date, get_all_ids


def export_matches_bronze(api_host_key: str, api_secert_key: str, league_id: str,
                          season_year: str, access_key: str,
                          secret_access: str) -> None:
    partidas = Fixtures(api_host_key, api_secert_key)
    data = partidas.get_data(league_id=league_id, season_year=season_year)

    S3WriterJson(Bucket.BRONZE, access_key, secret_access). \
        upload_fileobj(data, Storage.MATCHES, id=league_id)


def export_matches_silver(access_key: str, secret_access: str) -> None:
    data_partidas = FixturesTransformer(access_key=access_key,
                                        secret_access=secret_access
                                        )._get_transformation()

    S3WriterParquet(Bucket.SILVER, access_key, secret_access). \
        upload_fileobj(data_partidas, Storage.MATCHES)


def export_team_bronze(api_host_key: str, api_secert_key: str, access_key: str,
                       secret_access: str) -> None:
    times = Teams(api_host_key, api_secert_key)
    ids = get_all_ids(access_key, secret_access)
    data = times.get_data(team_id=ids)

    S3WriterJson(Bucket.BRONZE, access_key, secret_access). \
        upload_fileobj(data, Storage.TEAMS)


def export_team_silver(access_key: str, secret_access: str) -> None:
    data_partidas = TeamsTransformer(access_key=access_key,
                                     secret_access=secret_access
                                     )._get_transformation()

    S3WriterParquet(Bucket.SILVER, access_key, secret_access). \
        upload_fileobj(data_partidas, Storage.TEAMS)


def export_statistics_bronze(api_host_key: str, api_secert_key: str, date_from: date,
                             date_to: date, access_key: str,
                             secret_access: str) -> None:
    statistics = Matches(api_host_key, api_secert_key)
    ids = filter_by_date(access_key, secret_access, date_from, date_to)
    data = statistics.get_data(match_id=ids)

    S3WriterJson(Bucket.BRONZE, access_key, secret_access). \
        upload_fileobj(data, Storage.STATISTICS)


def export_statistics_silver(access_key: str, secret_access: str) -> None:
    data_partidas = MatchTransformer(access_key=access_key,
                                     secret_access=secret_access
                                     )._get_transformation()

    S3WriterParquet(Bucket.SILVER, access_key, secret_access). \
        upload_fileobj(data_partidas, Storage.STATISTICS)


def export_obt(access_key: str, secret_access: str) -> None:
    data = create_obt(access_key, secret_access)
    S3WriterParquet(Bucket.GOLD, access_key, secret_access).upload_fileobj(data, Storage.OBT)
