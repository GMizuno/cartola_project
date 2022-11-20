from datetime import date

from cartola.api import Fixtures, Teams, Matches
from database.athena import AthenaConn, Athena
from cartola.writer import S3Writer
from cartola.transformations import FixturesTransformer, TeamsTransformer, MatchTransformer


def export_matches_bronze(api_host_key: str, api_secert_key: str, league_id: str, season_year: str, access_key: str,
                          secret_access: str) -> None:
    partidas = Fixtures(api_host_key, api_secert_key)
    data = partidas.get_data(league_id=league_id, season_year=season_year)

    S3Writer('bootcamp-bronze', access_key, secret_access).upload_fileobj(data, 'matches', 'json', id=league_id)

    data_partidas = FixturesTransformer(access_key=access_key,
                                        secret_access=secret_access)._get_transformation()
    data_partidas = data_partidas[data_partidas['league_id'] == int(league_id)]

    S3Writer('bootcamp-silver', access_key, secret_access). \
        upload_fileobj(data_partidas, 'matches', 'parquet', id=league_id)


def export_team_bronze(api_host_key: str, api_secert_key: str, access_key: str,
                       secret_access: str) -> None:
    athena_conn = AthenaConn(access_key, secret_access)
    times = Teams(api_host_key, api_secert_key)
    ids = Athena(athena_conn).get_all_ids()
    data = times.get_data(team_id=ids)

    S3Writer('bootcamp-bronze', access_key, secret_access).upload_fileobj(data, 'teams', 'json')
    data_partidas = TeamsTransformer(access_key=access_key,
                                        secret_access=secret_access)._get_transformation()

    S3Writer('bootcamp-silver', access_key, secret_access). \
        upload_fileobj(data_partidas, 'teams', 'parquet')


def export_statistics_bronze(api_host_key: str, api_secert_key: str, date_from: date, date_to: date, access_key: str,
                             secret_access: str) -> None:
    athena_conn = AthenaConn(access_key, secret_access)
    statistics = Matches(api_host_key, api_secert_key)
    ids = Athena(athena_conn).filter_by_date(date_from, date_to)
    data = statistics.get_data(match_id=ids)

    S3Writer('bootcamp-bronze', access_key, secret_access).upload_fileobj(data, 'statistics', 'json')
    data_partidas = MatchTransformer(access_key=access_key,
                                        secret_access=secret_access)._get_transformation()

    S3Writer('bootcamp-silver', access_key, secret_access). \
        upload_fileobj(data_partidas, 'statistics', 'parquet')
