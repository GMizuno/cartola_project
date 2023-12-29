from typing import Any, List, Optional

from pydantic import BaseModel, Field, field_validator


class Parameters(BaseModel):
    fixture: str


class Paging(BaseModel):
    current: int
    total: int


class Team(BaseModel):
    id: int
    name: str = Field(exclude=True)
    logo: str = Field(exclude=True)
    update: str = Field(exclude=True)


class PlayerInfo(BaseModel):
    id: int
    name: str
    photo: Optional[str] = Field(exclude=True)


class Games(BaseModel):
    minutes: Optional[int]
    number: int
    position: str
    rating: Optional[str]
    captain: bool
    substitute: bool

    @field_validator('minutes')
    @classmethod
    def parse_none(cls, v) -> int:
        return v if v is not None else 0


class Shots(BaseModel):
    total: Optional[int]
    on: Optional[int]

    @field_validator('*')
    @classmethod
    def parse_none(cls, v) -> int:
        return v if v is not None else 0


class Goals(BaseModel):
    total: Optional[int]
    conceded: int
    assists: Optional[int]
    saves: Optional[int]

    @field_validator('*')
    @classmethod
    def parse_none(cls, v) -> int:
        return v if v is not None else 0


class Passes(BaseModel):
    total: Optional[int]
    key: Optional[int]
    accuracy: Optional[str]

    @field_validator('total', 'key')
    @classmethod
    def parse_none(cls, v) -> int:
        return v if v is not None else 0


class Tackles(BaseModel):
    total: Optional[int]
    blocks: Optional[int]
    interceptions: Optional[int]

    @field_validator('*')
    @classmethod
    def parse_none(cls, v) -> int:
        return v if v is not None else 0


class Duels(BaseModel):
    total: Optional[int]
    won: Optional[int]

    @field_validator('*')
    @classmethod
    def parse_none(cls, v) -> int:
        return v if v is not None else 0


class Dribbles(BaseModel):
    attempts: Optional[int]
    success: Optional[int]
    past: Optional[int]

    @field_validator('*')
    @classmethod
    def parse_none(cls, v) -> int:
        return v if v is not None else 0


class Fouls(BaseModel):
    drawn: Optional[int]
    committed: Optional[int]

    @field_validator('*')
    @classmethod
    def parse_none(cls, v) -> int:
        return v if v is not None else 0


class Cards(BaseModel):
    yellow: int
    red: int

    @field_validator('*')
    @classmethod
    def parse_none(cls, v) -> int:
        return v if v is not None else 0


class Penalty(BaseModel):
    won: Any
    commited: Any
    scored: int
    missed: int
    saved: Optional[int]

    @field_validator('*')
    @classmethod
    def parse_none(cls, v) -> int:
        return v if v is not None else 0


class Statistic(BaseModel):
    games: Games
    offsides: Optional[int]
    shots: Shots
    goals: Goals
    passes: Passes
    tackles: Tackles
    duels: Duels
    dribbles: Dribbles
    fouls: Fouls
    cards: Cards
    penalty: Penalty

    @field_validator('offsides')
    @classmethod
    def parse_none(cls, v) -> int:
        return v if v is not None else 0


class Player(BaseModel):
    player: PlayerInfo
    statistics: List[Statistic]


class ResponseItem(BaseModel):
    team: Team
    players: List[Player]


class PlayerItem(BaseModel):
    get: str = Field(exclude=True)
    parameters: Parameters
    errors: List
    results: int = Field(exclude=True)
    paging: Paging = Field(exclude=True)
    response: List[ResponseItem]


class Players(BaseModel):
    response: List[PlayerItem]
