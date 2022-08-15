from decouple import config

from api import Fixtures, Teams, Matches
from transformations import FixturesTransformer, TeamsTransformer
from writer import Writer

partidas = Fixtures(config('API_HOST_KEY'), config('API_SECERT_KEY'))
data = partidas.get_data(league_id="71", season_year="2022")

writer_partida = Writer('partida')
writer_partida.write_json(data=data)

match = FixturesTransformer('2022')
partida_parquet = match.get_data()

times = Teams(config('API_HOST_KEY'), config('API_SECERT_KEY'))
data = times.get_data(team_id=[['119', '120', '128', '129']])

writer_time = Writer('team')
writer_time.write_json(data=data)

times = TeamsTransformer('2022')
times_parquet = times.get_data()

