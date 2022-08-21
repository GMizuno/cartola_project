from decouple import config  # type: ignore
from datetime import date

from cartola.api import Fixtures, Teams, Matches
from cartola.transformations import FixturesTransformer, TeamsTransformer, MatchTransformer
from cartola.writer import Writer
from utils.util import get_some_match_id, get_all_team_id

## Partidas
partidas = Fixtures(config('API_HOST_KEY'), config('API_SECERT_KEY'))
data = partidas.get_data(league_id="71", season_year="2022")

Writer('matches').write_json(data=data)

FixturesTransformer().save_data()

# Times
times = Teams(config('API_HOST_KEY'), config('API_SECERT_KEY'))
id = get_all_team_id()
data = times.get_data(team_id=id)

Writer('teams').write_json(data=data)

TeamsTransformer().save_data()

## Estatisticas
partidas = Matches(config('API_HOST_KEY'), config('API_SECERT_KEY'))
id = get_some_match_id(date(2022, 8, 1), date(2022, 8, 10))
data = partidas.get_data(match_id=id)

Writer('statistics').write_json(data=data)

MatchTransformer().save_data()
