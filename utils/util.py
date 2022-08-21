from typing import List

import pandas as pd  # type: ignore
from dateutil import parser  # type: ignore
import datetime


def convert_time(time):
    time = parser.parse(time)
    return time.strftime("%d/%m/%Y %H:%M")


<<<<<<< HEAD
def get_all_team_id(filepath: str = 'matches/2022.parquet') -> List[str]:
=======
def get_all_team_id(filepath: str = 'partida/2022.parquet') -> List[str]:
>>>>>>> main
    return list(set(pd.read_parquet(filepath)['id_team_away'].to_list() + \
                    pd.read_parquet(filepath)['id_team_home'].to_list()))


<<<<<<< HEAD
def get_all_match_id(filepath: str = 'matches/2022.parquet') -> List[str]:
    return list(set(pd.read_parquet(filepath)['partida_id']))


def get_some_match_id(date_from: datetime.date, date_to: datetime.date, filepath: str = 'matches/2022.parquet') -> List[
=======
def get_all_match_id(filepath: str = 'partida/2022.parquet') -> List[str]:
    return list(set(pd.read_parquet(filepath)['partida_id']))


def get_some_match_id(date_from: datetime.date, date_to: datetime.date, filepath: str = 'partida/2022.parquet') -> List[
>>>>>>> main
    str]:
    data = pd.read_parquet(filepath)
    data = data.astype({'date': 'datetime64'})
    return data[data['date'].dt.date.between(date_from, date_to)]['partida_id'].to_list()
