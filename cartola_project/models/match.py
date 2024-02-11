from datetime import datetime
from typing import List, Optional

from dateutil import parser
from pydantic import BaseModel, Field, computed_field, field_validator


class Parameters(BaseModel):
    league: str
    season: str


class Paging(BaseModel):
    current: int
    total: int


class Periods(BaseModel):
    first:  Optional[int]
    second:  Optional[int]


class Venue(BaseModel):
    id: Optional[int]
    name: str
    city: str


class Status(BaseModel):
    long:  Optional[str]
    short:  Optional[str] = Field(exclude=True)
    elapsed:  Optional[int] = Field(exclude=True)


class Fixture(BaseModel):
    id: int
    referee:  Optional[str] = Field(exclude=True)
    timezone: str = Field(exclude=True)
    date:  Optional[str]
    timestamp:  Optional[int] = Field(exclude=True)
    periods: Periods = Field(exclude=True)
    venue: Venue = Field(exclude=True)
    status: Status

    @field_validator('date')
    @classmethod
    def _date(cls, date):
        time = parser.parse(date)
        return time.strftime("%d-%m-%Y %H:%M")

    @computed_field(alias='reference_date')
    def _reference_date(self) -> str:
        date = datetime.strptime(self.date, "%d-%m-%Y %H:%M")
        return date.strftime("%d-%m-%Y")


class League(BaseModel):
    id: int = Field(serialization_alias='league_id')
    name: str
    country: str = Field(exclude=True)
    logo: str = Field(exclude=True)
    flag: str = Field(exclude=True)
    season: int
    round: str = Field(serialization_alias='rodada')

    @field_validator('round')
    @classmethod
    def _round(cls, c):
        return c.replace('Regular Season - ', '').replace('Group Stage - ', '')


class Home(BaseModel):
    id: int = Field(serialization_alias='id_team_home')
    name: str = Field(exclude=True)
    logo: str = Field(exclude=True)
    winner: Optional[bool] = Field(serialization_alias='winner_home')


class Away(BaseModel):
    id: int = Field(serialization_alias='id_team_away')
    name: str = Field(exclude=True)
    logo: str = Field(exclude=True)
    winner: Optional[bool] = Field(serialization_alias='winner_away')


class Teams(BaseModel):
    home: Home = Field(exclude=True)
    away: Away = Field(exclude=True)

    @computed_field(alias='info')
    def info(self) -> dict:
        return self.home.model_dump(by_alias=True) | self.away.model_dump(by_alias=True)


class Goals(BaseModel):
    home: Optional[int] = Field(serialization_alias='goals_home')
    away: Optional[int] = Field(serialization_alias='goals_away')


class Halftime(BaseModel):
    home: Optional[int]
    away: Optional[int]


class Fulltime(BaseModel):
    home: Optional[int]
    away: Optional[int]


class Extratime(BaseModel):
    home: Optional[int]
    away: Optional[int]


class Penalty(BaseModel):
    home: Optional[int]
    away: Optional[int]


class Score(BaseModel):
    halftime: Halftime
    fulltime: Fulltime
    extratime: Extratime
    penalty: Penalty


class ResponseItem(BaseModel):
    fixture: Fixture
    league: League
    teams: Teams
    goals: Goals
    score: Score = Field(exclude=True)


class MatchItem(BaseModel):
    get: str = Field(exclude=True)
    parameters: Parameters = Field(exclude=True)
    errors: List
    results: int = Field(exclude=True)
    paging: Paging = Field(exclude=True)
    response: List[ResponseItem]


class Matches(BaseModel):
    responses: List[MatchItem]
