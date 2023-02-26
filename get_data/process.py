from datetime import date

import dask.dataframe as dd

from cartola_project import ParquetReader
from cartola_project.connector import CloudStorage
from .util import win_home, win


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
    dataframe["reference_date"] = dd.to_datetime(
        dataframe["reference_date"], format="%d-%m-%Y"
    )
    return dataframe.query(
        "reference_date >= @date_from and reference_date <= @date_to",
        local_dict={"date_from": date_from, "date_to": date_to},
    ).match_id.to_list()


def create_obt(cloudstorage: CloudStorage) -> None:
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
