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


def unlist(l: list):
    list(chain.from_iterable(l))


def _flatten_dict_gen(d: dict, parent_key: str, sep: str):
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            yield from flatten_dict(v, new_key, sep=sep).items()
        else:
            yield new_key, v


def flatten_dict(d: MutableMapping, parent_key: str = '', sep: str = '_'):
    return dict(_flatten_dict_gen(d, parent_key, sep))
