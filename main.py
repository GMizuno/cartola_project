from decouple import config  # type: ignore
from datetime import date

from cartola.api import Fixtures, Teams, Matches
from cartola.athena import Athena
from cartola.ingestor import Ingestor
from cartola.transformations import FixturesTransformer, TeamsTransformer, MatchTransformer

athena = Athena(config('AcessKey'), config('SecretKey'))

## Partidas - Brasileirao serie A
partidas = Fixtures(config('API_HOST_KEY'), config('API_SECERT_KEY'))
data = partidas.get_data(league_id="71", season_year="2022")

Ingestor('bootcamp-bronze', config('AcessKey'), config('SecretKey')).upload_fileobj(data, 'matches', 'json')

## Partidas - Premier League
partidas = Fixtures(config('API_HOST_KEY'), config('API_SECERT_KEY'))
data = partidas.get_data(league_id="39", season_year="2022")

Ingestor('bootcamp-bronze', config('AcessKey'), config('SecretKey')).upload_fileobj(data, 'matches', 'json')

## Times
times = Teams(config('API_HOST_KEY'), config('API_SECERT_KEY'))
id = athena.get_all_ids()
data = times.get_data(team_id=id)

Ingestor('bootcamp-bronze', config('AcessKey'), config('SecretKey')).upload_fileobj(data, 'teams', 'json')

## Estatisticas

partidas = Matches(config('API_HOST_KEY'), config('API_SECERT_KEY'))
id = athena.filter_by_date(date(2022, 9, 1), date(2022, 9, 7))
data = partidas.get_data(match_id=id)

Ingestor('bootcamp-bronze', config('AcessKey'), config('SecretKey')).upload_fileobj(data, 'statistics', 'json')

## Geral

data_partidas = FixturesTransformer(access_key=config('AcessKey'),
                                    secret_access=config('SecretKey'))._get_transformation()
data_teams = TeamsTransformer(access_key=config('AcessKey'), secret_access=config('SecretKey'))._get_transformation()
data_statistics = MatchTransformer(access_key=config('AcessKey'),
                                   secret_access=config('SecretKey'))._get_transformation()

Ingestor('bootcamp-silver', config('AcessKey'), config('SecretKey')).upload_fileobj(data_teams, 'matches', 'parquet')
Ingestor('bootcamp-silver', config('AcessKey'), config('SecretKey')).upload_fileobj(data_partidas, 'teams', 'parquet')
Ingestor('bootcamp-silver', config('AcessKey'), config('SecretKey')).upload_fileobj(data_statistics, 'statistics',
                                                                                    'parquet')

data_partidas_gold = FixturesTransformer(access_key=config('AcessKey'), secret_access=config('SecretKey'),
                                         bucket='bootcamp-silver')._get_transformation_gold()
data_teams_gold = TeamsTransformer(access_key=config('AcessKey'), secret_access=config('SecretKey'),
                                   bucket='bootcamp-silver')._get_transformation_gold()
data_statistics_gold = MatchTransformer(access_key=config('AcessKey'), secret_access=config('SecretKey'),
                                        bucket='bootcamp-silver')._get_transformation_gold()

Ingestor('bootcamp-gold', config('AcessKey'), config('SecretKey')). \
    upload_fileobj(data_partidas_gold, 'matches', 'parquet')
Ingestor('bootcamp-gold', config('AcessKey'), config('SecretKey')).\
    upload_fileobj(data_teams_gold, 'teams', 'parquet')
Ingestor('bootcamp-gold', config('AcessKey'), config('SecretKey')).\
    upload_fileobj(data_statistics_gold, 'statistics', 'parquet')
