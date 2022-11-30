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
