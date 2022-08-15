import pandas as pd
from cartola import Fixtures

querystring = {"league": "71", "season": "2022"}
fix = Fixtures("71", "2022")
rodadas = fix.fixtures()

from cartola import Teams

teams = Teams()
teste = teams.get_team('119')
teste = teams.get_teams(['119', '120'])

from cartola import Time

teste = Time('1/1/2022', '31/12/2022')
teste.create_time_tabel()

from cartola import Matches

teste = Matches()
rodada = teste.get_match('837992')
rodadas = teste.get_multiple_match(['837992','838021','838024'])

import pandas as pd
pd.DataFrame(rodada)

from cartola import Requester, Fixtures, Teams, Matches
from decouple import config


teste = Fixtures(config('API_HOST_KEY'), config('API_SECERT_KEY'), {"league": "71", "season": "2022"}, 'fixture')
# teste = Teams(config('API_HOST_KEY'), config('API_SECERT_KEY'), {"id": ['119', '120']}, 'team')
teste = Matches(config('API_HOST_KEY'), config('API_SECERT_KEY'), {"fixture": ['837992','838021','838024']}, 'match')
teste.get_all_match()


fix = Fixtures(config('API_HOST_KEY'), config('API_SECERT_KEY'), {"league": "71", "season": "2022"}, 'fixture').fixtures_dataframe()

fix.to_parquet('transformed/Fixtures.parquet')

