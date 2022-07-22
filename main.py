from requester import Fixtures

querystring = {"league": "71", "season": "2022"}
fix = Fixtures(querystring)
fix.query_parametres
rodadas = fix.fixtures()
