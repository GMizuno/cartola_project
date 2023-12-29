class FixturesTransformer(Transformer):
    def __init__(self, file: dict) -> None:
        self.file = file

    def extract_model(self):
        response = self.file[0].get('response')

        return [Match.from_dict(fixture) for fixture in response]

    def to_dataframe(self, list_model: list):
        data = pd.DataFrame([clean_dict_key(i) for i in list_model])

        data.rename(columns={'partida_id': 'match_id', 'rodada': 'round'}, inplace=True)
        data.replace(to_replace=r'Regular Season - ', value='', regex=True, inplace=True)
        data.replace(to_replace=r'Group Stage - ', value='', regex=True, inplace=True)

        return data.drop_duplicates()

    def _get_transformation(self) -> pd.DataFrame:
        fixture_json = []

        for fixture in self.extract_model():
            result = {
                'partida_id': fixture.fixture.id,
                'date': convert_time(fixture.fixture.date),
                'reference_date': convert_date(fixture.fixture.date),
                'rodada': fixture.league.round,
                'league_id': fixture.league.id,
                'id_team_away': fixture.teams.away.id,
                'id_team_home': fixture.teams.home.id,
                'goals_home': fixture.goals.home,
                'goals_away': fixture.goals.away,
                'winner_home': fixture.teams.home.winner,
                'winner_away': fixture.teams.away.winner,
            }
            fixture_json.append(result)

        return self.to_dataframe(fixture_json)
