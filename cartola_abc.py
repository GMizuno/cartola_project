import json

from decouple import config  # type: ignore
from datetime import date

from api import Fixtures, Teams, Matches
from transformations import FixturesTransformer, TeamsTransformer
from writer import Writer
from utils.util import get_all_team_id,get_all_match_id,get_some_match_id  # type: ignore

partidas = Fixtures(config('API_HOST_KEY'), config('API_SECERT_KEY'))
data = partidas.get_data(league_id="71", season_year="2022")

writer_partida = Writer('macthes')
writer_partida.write_json(data=data)

match = FixturesTransformer('2022')
partida_parquet = match.get_data()


times = Teams(config('API_HOST_KEY'), config('API_SECERT_KEY'))
id = get_all_team_id()
data = times.get_data(team_id=id)

writer_time = Writer('team')
writer_time.write_json(data=data)

team = TeamsTransformer('2022')
team_parquet = team.get_data()

partidas = Matches(config('API_HOST_KEY'), config('API_SECERT_KEY'))
id = get_some_match_id(date(2022,8,1), date(2022,8,10))
data = partidas.get_data(match_id=id)

Writer('statistics').write_json(data=data)
