from pyathena import connect
from pyathena.common import BaseCursor
from pyathena.pandas.cursor import PandasCursor
import pandas as pd


class AthenaConn:

    def __init__(self, access_key: str, secret_access: str, bucket: str = "s3://staging-area-cartola/",
                 region_name: str = "us-east-1") -> None:
        self.region_name = region_name
        self.bucket = bucket
        self.secret_access = secret_access
        self.access_key = access_key

    def connect(self) -> BaseCursor:
        return connect(s3_staging_dir=self.bucket,
                       aws_access_key_id=self.access_key,
                       aws_secret_access_key=self.secret_access,
                       region_name=self.region_name,
                       cursor_class=PandasCursor).cursor()


class Athena:

    def __init__(self, conn: AthenaConn):
        self.conn = conn.connect()

    def execute_query(self, query: str) -> pd.DataFrame:
        return self.conn.execute(query).as_pandas()

    def get_all_ids(self) -> list:
        data = self.execute_query("SELECT distinct id_team_home FROM cartola_siver.matches")
        return data['id_team_home'].to_list()

    def filter_by_date(self, date_from, date_to, query: str = None) -> list:
        if query is not None:
            data = self.execute_query(query)
        else:
            data = self.execute_query("SELECT * FROM cartola_siver.matches")

        if 'reference_date' and 'match_id' not in data.columns:
            raise KeyError('reference_date or partida_id are not a column')

        data["reference_date"] = pd.to_datetime(data["reference_date"], format='%d-%m-%Y')
        data_filter = data[data['reference_date'].dt.date.between(date_from, date_to)]
        ids = data_filter['match_id'].to_list()
        print(data_filter)

        return ids

    def create_obt(self):
        data = self.execute_query("""\
                select statistics.*,
                       matches.date,
                       matches.reference_date,
                       matches.round,
                       matches.id_team_away,
                       matches.id_team_home,
                       matches.league_id,
                       teams.name,
                       teams.code,
                       teams.country,
                       teams.logo,
                       teams.city,
                       teams.state
                from cartola_siver.statistics
                join cartola_siver.matches on statistics.match_id = matches.match_id
                join cartola_siver.teams on statistics.team_id = cast(teams.team_id as int)
        """)

        return data
