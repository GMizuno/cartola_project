from typing import List

import pandas as pd  # type: ignore
from dateutil import parser  # type: ignore
import datetime


def convert_time(time):
    time = parser.parse(time)
    return time.strftime("%d/%m/%Y %H:%M")


def get_all_team_id(filepath: str = 'matches/2022.parquet') -> List[str]:
    return list(set(pd.read_parquet(filepath)['id_team_away'].to_list() + \
                    pd.read_parquet(filepath)['id_team_home'].to_list()))


def get_all_match_id(filepath: str = 'matches/2022.parquet') -> List[str]:
    return list(set(pd.read_parquet(filepath)['partida_id']))


def get_all_match_id(filepath: str = 'matches/2022.parquet') -> List[str]:
    return list(set(pd.read_parquet(filepath)['partida_id']))


def get_some_match_id(date_from: datetime.date,
                      date_to: datetime.date,
                      filepath: str = 'matches/2022.parquet') -> List[str]:
    data = pd.read_parquet(filepath).astype({'date': 'datetime64'})
    return data[data['date'].dt.date.between(date_from, date_to)]['partida_id'].to_list()

def remove_white_space_key(dictionary: dict):
    new_keys = ["_".join(x.split()) for x in dictionary]
    return dict(zip(new_keys, list(dictionary.values())))

def remove_special_letter(dictionary: dict):
    new_keys = [x.replace("%", 'percentage') for x in dictionary]
    return dict(zip(new_keys, list(dictionary.values())))

def clean_dict_key(dictionary: dict):
    return clean_dict_key(remove_white_space_key(dictionary))