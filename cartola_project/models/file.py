from enum import Enum


class File(str, Enum):
    PARQUET = 'parquet'
    JSON = 'json'
