from requester import Fixtures

querystring = {"league": "71", "season": "2022"}
fix = Fixtures("71", "2022")
rodadas = fix.fixtures()

from requester import Teams

teams = Teams()
teste = teams.get_team('119')
teste = teams.get_teams(['119', '120'])