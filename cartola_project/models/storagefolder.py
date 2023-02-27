from enum import Enum


class StorageFolder(str, Enum):
    MATCHES = 'matches'
    TEAMS = 'teams'
    STATISTICS = 'statistics'
    OBT = 'obt'
    PLAYERS = 'players'
