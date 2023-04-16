from dataclasses import dataclass
from typing import Any


@dataclass
class Away:
    id: int
    name: str
    logo: str
    winner: bool

    @staticmethod
    def from_dict(obj: Any) -> 'Away':
        _id = int(obj.get("id"))
        _name = str(obj.get("name"))
        _logo = str(obj.get("logo"))
        _winner = obj.get("winner")
        return Away(_id, _name, _logo, _winner)


@dataclass
class Home:
    id: int
    name: str
    logo: str
    winner: bool

    @staticmethod
    def from_dict(obj: Any) -> 'Home':
        _id = int(obj.get("id"))
        _name = str(obj.get("name"))
        _logo = str(obj.get("logo"))
        _winner = obj.get("winner")
        return Home(_id, _name, _logo, _winner)


@dataclass
class Teams:
    home: Home
    away: Away

    @staticmethod
    def from_dict(obj: Any) -> 'Club':
        _home = Home.from_dict(obj.get("home"))
        _away = Away.from_dict(obj.get("away"))
        return Teams(_home, _away)


@dataclass
class League:
    id: int
    name: str
    country: str
    logo: str
    flag: str
    season: int
    round: str

    @staticmethod
    def from_dict(obj: Any) -> 'League':
        _id = int(obj.get("id"))
        _name = str(obj.get("name"))
        _country = str(obj.get("country"))
        _logo = str(obj.get("logo"))
        _flag = str(obj.get("flag"))
        _season = int(obj.get("season"))
        _round = str(obj.get("round"))
        return League(_id, _name, _country, _logo, _flag, _season, _round)


@dataclass
class Goals:
    home: int
    away: int

    @staticmethod
    def from_dict(obj: Any) -> 'Goals':
        _home = int(obj.get("home") or 0)
        _away = int(obj.get("away") or 0)
        return Goals(_home, _away)


@dataclass
class Extratime:
    home: int
    away: int

    @staticmethod
    def from_dict(obj: Any) -> 'Extratime':
        _home = int(obj.get("home") or 0)
        _away = int(obj.get("away") or 0)
        return Extratime(_home, _away)


@dataclass
class Fulltime:
    home: int
    away: int

    @staticmethod
    def from_dict(obj: Any) -> 'Fulltime':
        _home = int(obj.get("home") or 0)
        _away = int(obj.get("away") or 0)
        return Fulltime(_home, _away)


@dataclass
class Halftime:
    home: int
    away: int

    @staticmethod
    def from_dict(obj: Any) -> 'Halftime':
        _home = int(obj.get("home") or 0)
        _away = int(obj.get("away") or 0)
        return Halftime(_home, _away)


@dataclass
class Penalty:
    home: int
    away: int

    @staticmethod
    def from_dict(obj: Any) -> 'Penalty':
        _home = int(obj.get("home") or 0)
        _away = int(obj.get("away") or 0)
        return Penalty(_home, _away)


@dataclass
class Score:
    halftime: Halftime
    fulltime: Fulltime
    extratime: Extratime
    penalty: Penalty

    @staticmethod
    def from_dict(obj: Any) -> 'Score':
        _halftime = Halftime.from_dict(obj.get("halftime"))
        _fulltime = Fulltime.from_dict(obj.get("fulltime"))
        _extratime = Extratime.from_dict(obj.get("extratime"))
        _penalty = Penalty.from_dict(obj.get("penalty"))
        return Score(_halftime, _fulltime, _extratime, _penalty)


@dataclass
class Periods:
    first: int
    second: int

    @staticmethod
    def from_dict(obj: Any) -> 'Periods':
        _first = int(obj.get("first") or 0)
        _second = int(obj.get("second") or 0)
        return Periods(_first, _second)


@dataclass
class Status:
    long: str
    short: str
    elapsed: int

    @staticmethod
    def from_dict(obj: Any) -> 'Status':
        _long = str(obj.get("long"))
        _short = str(obj.get("short"))
        _elapsed = int(obj.get("elapsed") or 0)
        return Status(_long, _short, _elapsed)


@dataclass
class Venue:
    id: int
    name: str
    city: str

    @staticmethod
    def from_dict(obj: Any) -> 'Venue':
        _id = int(obj.get("id") or 0)
        _name = str(obj.get("name"))
        _city = str(obj.get("city"))
        return Venue(_id, _name, _city)


# TODO: Add this class in Match
@dataclass
class Parameters:
    league: str
    season: str

    @staticmethod
    def from_dict(obj: Any) -> 'Parameters':
        _league = str(obj.get("league"))
        _season = str(obj.get("season"))
        return Parameters(_league, _season)


@dataclass
class Fixture:
    id: int
    referee: str
    timezone: str
    date: str
    timestamp: int
    periods: Periods
    venue: Venue
    status: Status

    @staticmethod
    def from_dict(obj: Any) -> 'Fixture':
        _id = int(obj.get("id") or 0)
        _referee = str(obj.get("referee"))
        _timezone = str(obj.get("timezone"))
        _date = str(obj.get("date"))
        _timestamp = int(obj.get("timestamp") or 0)
        _periods = Periods.from_dict(obj.get("periods"))
        _venue = Venue.from_dict(obj.get("venue"))
        _status = Status.from_dict(obj.get("status"))
        return Fixture(_id, _referee, _timezone, _date, _timestamp, _periods,
                       _venue, _status)


@dataclass
class Match:
    fixture: Fixture
    league: League
    teams: Teams
    goals: Goals
    score: Score

    @staticmethod
    def from_dict(obj: Any) -> 'Match':
        _fixture = Fixture.from_dict(obj.get("fixture"))
        _league = League.from_dict(obj.get("league"))
        _teams = Teams.from_dict(obj.get("teams"))
        _goals = Goals.from_dict(obj.get("goals"))
        _score = Score.from_dict(obj.get("score"))

        return Match(_fixture, _league, _teams, _goals, _score)
