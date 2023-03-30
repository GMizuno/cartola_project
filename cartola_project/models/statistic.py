from dataclasses import dataclass
from typing import Any

from dataclasses_json import dataclass_json

@dataclass_json
@dataclass
class Statistics:
    ShotsonGoal: int
    ShotsoffGoal: int
    TotalShots: int
    BlockedShots: int
    Shotsinsidebox: int
    Shotsoutsidebox: int
    Fouls: int
    CornerKicks: int
    Offsides: int
    BallPossession: str
    YellowCards: int
    RedCards: int
    GoalkeeperSaves: int
    Totalpasses: int
    Passesaccurate: int
    Passesperc: str

    @staticmethod
    def from_dict(obj: Any) -> 'Statistics':
        _ShotsonGoal = int(obj.get("Shots on Goal") or 0)
        _ShotsoffGoal = int(obj.get("Shots off Goal") or 0)
        _TotalShots = int(obj.get("Total Shots") or 0)
        _BlockedShots = int(obj.get("Blocked Shots") or 0)
        _Shotsinsidebox = int(obj.get("Shots insidebox") or 0)
        _Shotsoutsidebox = int(obj.get("Shots outsidebox") or 0)
        _Fouls = int(obj.get("Fouls") or 0)
        _CornerKicks = int(obj.get("Corner Kicks") or 0)
        _Offsides = int(obj.get("Offsides") or 0)
        _BallPossession = str(obj.get("Ball Possession"))
        _YellowCards = int(obj.get("Yellow Cards") or 0)
        _RedCards = int(obj.get("Red Cards") or 0)
        _GoalkeeperSaves = int(obj.get("Goalkeeper Saves") or 0)
        _Totalpasses = int(obj.get("Total passes") or 0)
        _Passesaccurate = int(obj.get("Passes accurate") or 0)
        _Passesperc = str(obj.get("Passes %"))
        return Statistics(_ShotsonGoal, _ShotsoffGoal, _TotalShots,
                          _BlockedShots, _Shotsinsidebox, _Shotsoutsidebox,
                          _Fouls, _CornerKicks, _Offsides, _BallPossession,
                          _YellowCards, _RedCards, _GoalkeeperSaves,
                          _Totalpasses, _Passesaccurate, _Passesperc, )


@dataclass_json
@dataclass
class Team:
    id: int
    name: str
    logo: str

    @staticmethod
    def from_dict(obj: Any) -> 'Team':
        _id = int(obj.get("id"))
        _name = str(obj.get("name"))
        _logo = str(obj.get("logo"))
        return Team(_id, _name, _logo)

@dataclass_json
@dataclass
class TeamStatistics:
    statistics: Statistics
    team: Team
    fixture: str

    @staticmethod
    def from_dict(obj: Any) -> 'TeamStatistics':
        _statistics = Statistics.from_dict(obj.get("statistics"))
        _team = Team.from_dict(obj.get("team"))
        _fixture = str(obj.get("fixture"))
        return TeamStatistics(_statistics, _team, _fixture)
