from decouple import config  # type: ignore
from datetime import date

from cartola.api import Fixtures, Teams, Matches
from cartola.transformations import FixturesTransformer, TeamsTransformer, MatchTransformer
from cartola.writer import Writer, WriterGCP
from utils.util import get_some_match_id, get_all_team_id_from_league

## Partidas - Brasileirao serie A
partidas = Fixtures(config('API_HOST_KEY'), config('API_SECERT_KEY'))
data = partidas.get_data(league_id="71", season_year="2022")

Writer('matches').write_json(data=data)

## Times - Brasileirao serie A
times = Teams(config('API_HOST_KEY'), config('API_SECERT_KEY'))
id = get_all_team_id_from_league(71)
data = times.get_data(team_id=id)

Writer('teams').write_json(data=data)

## Partidas - Premier League
partidas = Fixtures(config('API_HOST_KEY'), config('API_SECERT_KEY'))
data = partidas.get_data(league_id="39", season_year="2022")

Writer('matches').write_json(data=data)

## Times - Premier League
times = Teams(config('API_HOST_KEY'), config('API_SECERT_KEY'))
id = get_all_team_id_from_league(39)
data = times.get_data(team_id=id)

Writer('teams').write_json(data=data)

## Salvando em parquet e enviando para GCP
FixturesTransformer().save_data()
TeamsTransformer().save_data()

WriterGCP(bucket='cartola_raw', parent_folder='matches', project_id='cartola-360814').upload_from_directory()
WriterGCP(bucket='cartola_silver', parent_folder='matches', project_id='cartola-360814').upload_from_directory(extention='parquet')
WriterGCP(bucket='cartola_raw', parent_folder='teams', project_id='cartola-360814').upload_from_directory()
WriterGCP(bucket='cartola_silver', parent_folder='teams', project_id='cartola-360814').upload_from_directory(extention='parquet')

## Estatisticas

partidas = Matches(config('API_HOST_KEY'), config('API_SECERT_KEY'))
id = get_some_match_id(date(2022, 8, 1), date(2022, 8, 31))
data = partidas.get_data(match_id=[867946])

Writer('statistics').write_json(data=data)

MatchTransformer().save_data()

WriterGCP(bucket='cartola_raw', parent_folder='statistics', project_id='cartola-360814').upload_from_directory()
WriterGCP(bucket='cartola_silver', parent_folder='statistics', project_id='cartola-360814').\
    upload_from_directory(extention='parquet')