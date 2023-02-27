from datetime import date

from decouple import config

from get_data.export import export_player_bronze, export_player_silver


def config_statistics_dict(league_id: str,
                           season_year: str,
                           date_from: date,
                           date_to: date,
                           api_host_key: str = 'API_HOST_KEY',
                           api_secert_key: str = 'API_SECERT_KEY',
                           ) -> dict:
    return {
        'api_host_key': config(api_host_key),
        'api_secert_key': config(api_secert_key),
        'league_id': league_id,
        'season_year': season_year,
        'date_from': date_from,
        'date_to': date_to,
    }


# TODO: Mover essa parte,
params = [
    config_statistics_dict('39', '2022', date(2022, 10, 1), date(2022, 10, 12)),
]
for param in params:
    result = export_player_bronze(**param)
    export_player_silver(result, param['league_id'], param['season_year'])
