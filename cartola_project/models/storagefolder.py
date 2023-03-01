from enum import Enum


class StorageFolder(str, Enum):
    MATCHES = 'matches'
    TEAMS = 'teams'
    STATISTICS = 'statistics'
    OBT = 'obt'
    OBT_MATCHES = 'obt_matches'
    OBT_PLAYERS = 'obt_players'
    PLAYERS = 'players'
