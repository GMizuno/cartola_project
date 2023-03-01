from datetime import date

import pandas as pd
from pandas import DataFrame

from cartola_project import ParquetReader
from cartola_project.connector import CloudStorage


def win_home(data: DataFrame):
    if data.goals_home == data.goals_away:
        return "home_draw"
    elif data.goals_home > data.goals_away:
        return "home_win"
    else:
        return "home_lose"


def win(data: DataFrame):
    if data.win_home == "home_draw":
        return "draw"
    elif data.win_home == "home_win" and data.home == True:
        return "win"
    else:
        return "lose"


def get_all_ids(cloudstorage: CloudStorage, league_id: str,
                season_year: str) -> list:
    d = ParquetReader(
        cloudstorage,
        f"teste_cartola_gabriel",
        f"matches/silver/league={league_id}/season={season_year}/",
    ).read_all_files()
    return list(
        set(
            d.id_team_home.drop_duplicates().to_list()
            + d.id_team_away.drop_duplicates().to_list()
        )
    )


# TODO: Remover Dask dessa funcao
def filter_by_date(
        cloudstorage: CloudStorage,
        league_id: str,
        season_year: str,
        date_from: date,
        date_to: date,
):
    dataframe = ParquetReader(
        cloudstorage,
        f"teste_cartola_gabriel",
        f"matches/silver/league={league_id}/season={season_year}/",
    ).read_all_files()

    dataframe["reference_date"] = pd.to_datetime(dataframe['date']).dt.date
    result = dataframe.loc[
        (dataframe['reference_date'] >= date_from) &
        (dataframe['reference_date'] <= date_to)]. \
        match_id.to_list()
    return result


def create_obt_matches(cloudstorage: CloudStorage) -> None:
    dataframe1 = ParquetReader(
        cloudstorage, f"teste_cartola_gabriel", f"matches/silver/"
    ).read_all_files()

    dataframe2 = ParquetReader(
        cloudstorage, f"teste_cartola_gabriel", f"statistics/silver/"
    ).read_all_files()

    dataframe3 = ParquetReader(
        cloudstorage, f"teste_cartola_gabriel", f"teams/silver/"
    ).read_all_files()

    dataframe3 = dataframe3.astype({"team_id": "int64"})

    result = dataframe1.merge(dataframe2, how="inner", on=["match_id"])
    result = result.merge(dataframe3, how="inner", on=["team_id"])

    result = result.assign(home=result.id_team_home == result.team_id)
    result["win_home"] = result.apply(win_home, axis=1)
    result["win"] = result.apply(win, axis=1)
    result = result.drop(columns="win_home")

    return result.drop_duplicates()


def create_obt_players(cloudstorage: CloudStorage) -> None:
    dataframe1 = ParquetReader(
        cloudstorage, f"teste_cartola_gabriel", f"players/silver/"
    ).read_all_files()

    dataframe2 = ParquetReader(
        cloudstorage, f"teste_cartola_gabriel", f"matches/silver/"
    ).read_all_files()

    dataframe2 = dataframe2.astype({"match_id": "int64"})
    dataframe1 = dataframe1.astype({"match_id": "int64"})

    result = dataframe1.merge(dataframe2, how="inner", on=["match_id"])

    return result.drop_duplicates()
