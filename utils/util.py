from typing import List
import glob
import os

import pandas as pd  # type: ignore
from dateutil import parser  # type: ignore
import datetime


def convert_time(time):
    time = parser.parse(time)
    return time.strftime("%d/%m/%Y %H:%M")


def convert_date(time):
    time = parser.parse(time)
    return time.strftime("%d/%m/%Y")


def get_all_team_id() -> List[str]:
    list_of_files = glob.glob(f'matches/*.parquet')
    filepath = max(list_of_files, key=os.path.getctime)
    return list(set(pd.read_parquet(filepath)['id_team_away'].to_list() + \
                pd.read_parquet(filepath)['id_team_home'].to_list()))


def get_all_match_id() -> List[str]:
    list_of_files = glob.glob(f'matches/*.parquet')
    filepath = max(list_of_files, key=os.path.getctime)
    return list(set(pd.read_parquet(filepath)['partida_id']))


def get_some_match_id(date_from: datetime.date,
                      date_to: datetime.date) -> List[str]:
    list_of_files = glob.glob(f'matches/*.parquet')
    filepath = max(list_of_files, key=os.path.getctime)
    data = pd.read_parquet(filepath)
    data["reference_date"] = pd.to_datetime(data["reference_date"], format='%d/%m/%Y')
    return data[data['reference_date'].dt.date.between(date_from, date_to)]['partida_id'].to_list()


def remove_white_space_key(dictionary: dict):
    new_keys = ["_".join(x.split()) for x in dictionary]
    return dict(zip(new_keys, list(dictionary.values())))


def remove_special_letter(dictionary: dict):
    new_keys = [x.replace("%", 'percentage') for x in dictionary]
    return dict(zip(new_keys, list(dictionary.values())))


def clean_dict_key(dictionary: dict):
    return remove_special_letter(remove_white_space_key(dictionary))
