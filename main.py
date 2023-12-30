from decouple import config

from cartola_project import Fixtures, Matches, Players, Teams
from cartola_project.transformations import (
    MatchTransformer,
    PlayerTransformer,
    StatisticsTransformer,
    TeamTransformer,
)

api_host_key = config("API_HOST_KEY")
api_secert_key = config("API_SECERT_KEY")

partidas = Fixtures(api_host_key, api_secert_key)
stats = Matches(api_host_key, api_secert_key)
players = Players(api_host_key, api_secert_key)
teams = Teams(api_host_key, api_secert_key)


data_partidas = partidas.get_data(league_id='71', season_year='2023')
data_stats = stats.get_data(match_id=['1005649'])
data_players = players.get_data(match_id=['1005649'])
data_teams = teams.get_data(team_id=['126'])


data_partidas_clean = MatchTransformer(data_partidas).transformation()
data_stats_clean = PlayerTransformer(data_players).transformation()
data_players_clean = StatisticsTransformer(data_stats).transformation()
data_teams_clean = TeamTransformer(data_teams).transformation()
