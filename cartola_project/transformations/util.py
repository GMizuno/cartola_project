from collections.abc import MutableMapping
from itertools import chain

from dateutil import parser


def convert_date(time):
    time = parser.parse(time)
    return time.strftime("%d-%m-%Y")


def convert_time(time):
    time = parser.parse(time)
    return time.strftime("%d-%m-%Y %H:%M")


def remove_white_space_key(dictionary: dict) -> dict:
    new_keys = ["_".join(x.split()) for x in dictionary]
    return dict(zip(new_keys, list(dictionary.values())))


def remove_special_letter(dictionary: dict) -> dict:
    new_keys = [x.replace("%", 'percentage') for x in dictionary]
    return dict(zip(new_keys, list(dictionary.values())))


def clean_dict_key(dictionary: dict):
    return remove_special_letter(remove_white_space_key(dictionary))


def unlist(l: list) -> list:
    return list(chain.from_iterable(l))


def _flatten_dict_gen(d: dict, parent_key: str, sep: str):
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            yield from flatten_dict(v, new_key, sep=sep).items()
        else:
            yield new_key, v


def flatten_dict(d: MutableMapping, parent_key: str = '', sep: str = '_'):
    return dict(_flatten_dict_gen(d, parent_key, sep))


def tranform_stats(stat: dict) -> dict:
    stat_new_key = ''
    stat_new_value = None
    for key, value in stat.items():
        if key == 'type':
            stat_new_key = value.replace(" ", "").replace('%', 'perc')
        elif key == 'value':
            stat_new_value = value
        else:
            raise ValueError('Invalid key/value pair')

    return {stat_new_key: stat_new_value}


def merge_dict(list_dict: list) -> dict:
    new_dict = {}
    for item in list_dict:
        new_dict.update(item)
    return new_dict
