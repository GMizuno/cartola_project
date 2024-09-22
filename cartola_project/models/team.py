from typing import List, Optional

from pydantic import BaseModel, Field, computed_field, field_validator


class Parameters(BaseModel):
    id: str


class Paging(BaseModel):
    current: int
    total: int


class Team(BaseModel):
    id: int = Field(..., serialization_alias='team_id')
    name: str
    code: Optional[str]
    country: str
    founded: int = Field(exclude=True)
    national: bool = Field(exclude=True)
    logo: str


class Venue(BaseModel):
    id: int
    name: str = Field(exclude=True)
    address: Optional[str] = Field(exclude=True)
    city: str
    capacity: int = Field(exclude=True)
    surface: str = Field(exclude=True)
    image: str = Field(exclude=True)

    @computed_field
    def state(self) -> str:
        return ''

    @field_validator('city')
    @classmethod
    def _city(cls, c):
        if len(c.split(',')) >= 1:
            return c.split(',')[0].strip()
        return ''


class ResponseItem(BaseModel):
    team: Team
    venue: Venue


class TeamItem(BaseModel):
    get: str
    parameters: Parameters
    errors: List
    results: int = Field(exclude=True)
    paging: Paging = Field(exclude=True)
    response: List[ResponseItem]


class Teams(BaseModel):
    responses: List[TeamItem]
