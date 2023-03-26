from dataclasses import dataclass
from typing import Any
from typing import List

from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class Cards:
    yellow: int
    red: int

    @staticmethod
    def from_dict(obj: Any) -> 'Cards':
        _yellow = int(obj.get("yellow") or 0)
        _red = int(obj.get("red") or 0)
        return Cards(_yellow, _red)


@dataclass_json
@dataclass
class Dribbles:
    attempts: int
    success: int
    past: int

    @staticmethod
    def from_dict(obj: Any) -> 'Dribbles':
        _attempts = int(obj.get("attempts") or 0)
        _success = int(obj.get("success") or 0)
        _past = int(obj.get("past") or 0)
        return Dribbles(_attempts, _success, _past)


@dataclass_json
@dataclass
class Duels:
    total: int
    won: int

    @staticmethod
    def from_dict(obj: Any) -> 'Duels':
        _total = int(obj.get("total") or 0)
        _won = int(obj.get("won") or 0)
        return Duels(_total, _won)


@dataclass
class Fouls:
    drawn: int
    committed: int

    @staticmethod
    def from_dict(obj: Any) -> 'Fouls':
        _drawn = int(obj.get("drawn") or 0)
        _committed = int(obj.get("committed") or 0)
        return Fouls(_drawn, _committed)


@dataclass_json
@dataclass
class Games:
    minutes: int
    number: int
    position: str
    rating: object
    captain: bool
    substitute: bool

    @staticmethod
    def from_dict(obj: Any) -> 'Games':
        _minutes = int(obj.get("minutes") or 0)
        _number = int(obj.get("number") or 0)
        _position = str(obj.get("position"))
        _rating = str(obj.get("rating"))
        _captain = obj.get("captain")
        _substitute = obj.get("substitute")
        return Games(_minutes, _number, _position, _rating, _captain,
                     _substitute)


@dataclass_json
@dataclass
class Goals:
    total: int
    conceded: int
    assists: int
    saves: int

    @staticmethod
    def from_dict(obj: Any) -> 'Goals':
        _total = int(obj.get("total") or 0)
        _conceded = int(obj.get("conceded") or 0)
        _assists = int(obj.get("assists") or 0)
        _saves = int(obj.get("saves") or 0)
        return Goals(_total, _conceded, _assists, _saves)


@dataclass_json
@dataclass
class Passes:
    total: int
    key: int
    accuracy: object

    @staticmethod
    def from_dict(obj: Any) -> 'Passes':
        _total = int(obj.get("total") or 0)
        _key = int(obj.get("key") or 0)
        _accuracy = str(obj.get("accuracy"))
        return Passes(_total, _key, _accuracy)


@dataclass_json
@dataclass
class Penalty:
    won: int
    commited: int
    scored: int
    missed: int
    saved: int

    @staticmethod
    def from_dict(obj: Any) -> 'Penalty':
        _won = int(obj.get("won") or 0)
        _commited = int(obj.get("commited") or 0)
        _scored = int(obj.get("scored") or 0)
        _missed = int(obj.get("missed") or 0)
        _saved = int(obj.get("saved") or 0)
        return Penalty(_won, _commited, _scored, _missed, _saved)


@dataclass_json
@dataclass
class Player:
    id: int
    name: str
    photo: str

    @staticmethod
    def from_dict(obj: Any) -> 'Player':
        _id = obj.get("id")
        _name = str(obj.get("name"))
        _photo = str(obj.get("photo"))
        return Player(_id, _name, _photo)


@dataclass_json
@dataclass
class Tackles:
    total: int
    blocks: int
    interceptions: int

    @staticmethod
    def from_dict(obj: Any) -> 'Tackles':
        _total = int(obj.get("total") or 0)
        _blocks = int(obj.get("blocks") or 0)
        _interceptions = int(obj.get("interceptions") or 0)
        return Tackles(_total, _blocks, _interceptions)


@dataclass_json
@dataclass
class Shots:
    total: int
    on: int

    @staticmethod
    def from_dict(obj: Any) -> 'Shots':
        _total = int(obj.get("total") or 0)
        _on = int(obj.get("on") or 0)
        return Shots(_total, _on)


@dataclass_json
@dataclass
class Statistic:
    games: Games
    offsides: int
    shots: Shots
    goals: Goals
    passes: Passes
    tackles: Tackles
    duels: Duels
    dribbles: Dribbles
    fouls: Fouls
    cards: Cards
    penalty: Penalty

    @staticmethod
    def from_dict(obj: Any) -> 'Statistic':
        _games = Games.from_dict(obj.get("games"))
        _offsides = int(obj.get("offsides") or 0)
        _shots = Shots.from_dict(obj.get("shots"))
        _goals = Goals.from_dict(obj.get("goals"))
        _passes = Passes.from_dict(obj.get("passes"))
        _tackles = Tackles.from_dict(obj.get("tackles"))
        _duels = Duels.from_dict(obj.get("duels"))
        _dribbles = Dribbles.from_dict(obj.get("dribbles"))
        _fouls = Fouls.from_dict(obj.get("fouls"))
        _cards = Cards.from_dict(obj.get("cards"))
        _penalty = Penalty.from_dict(obj.get("penalty"))
        return Statistic(_games, _offsides, _shots, _goals, _passes, _tackles,
                         _duels, _dribbles, _fouls, _cards, _penalty)


@dataclass_json
@dataclass
class Players:
    player: Player
    statistics: List[Statistic]

    @staticmethod
    def from_dict(obj: Any) -> 'Players':
        _player = Player.from_dict(obj.get("player"))
        _statistics = [Statistic.from_dict(y) for y in obj.get("statistics")]
        return Players(_player, _statistics)


@dataclass_json
@dataclass
class Team:
    id: int
    name: str
    logo: str
    update: str

    @staticmethod
    def from_dict(obj: Any) -> 'Team':
        _id = int(obj.get("id"))
        _name = str(obj.get("name"))
        _logo = str(obj.get("logo"))
        _update = str(obj.get("update"))
        return Team(_id, _name, _logo, _update)


@dataclass
class Info:
    team: Team
    fixture: str
    players: List[Players]

    @staticmethod
    def from_dict(obj: Any) -> 'Info':
        _team = Team.from_dict(obj.get("team"))
        _fixture = str(obj.get("fixture"))
        _players = [Players.from_dict(y) for y in obj.get("players")]
        return Info(_team, _fixture, _players)


@dataclass_json
@dataclass
class StatisticsPlayer:
    team: Team
    fixture: str
    player: Player
    statistics: Statistic
