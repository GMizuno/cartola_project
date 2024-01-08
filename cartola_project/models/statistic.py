from typing import Optional

from pydantic import BaseModel, Field, computed_field


class Parameters(BaseModel):
    fixture: str = Field(..., serialization_alias='match_id')


class Paging(BaseModel):
    current: int
    total: int


class Team(BaseModel):
    id: int = Field(..., serialization_alias='team_id')
    name: str = Field(exclude=True)
    logo: str = Field(exclude=True)


class Statistic(BaseModel):
    type: str = Field(exclude=True)
    value: Optional[int | str]

    @computed_field(alias='stats')
    def stats(self) -> dict:
        value = self.value if self.value is not None else 0
        if self.type in ['Passes %', 'Ball Possession']:
            value = int(value.replace('%', ''))
        key = f"statistics_{self.type.replace(' ', '')}"
        return {key: value}


class ResponseItem(BaseModel):
    team: Team
    statistics: list[Statistic] = Field(exclude=True)

    @computed_field(alias='stats')
    def stats(self) -> dict:
        data = [s.stats for s in self.statistics]
        merged_dict = {}
        for d in data:
            merged_dict.update(d)
        return merged_dict


class StatisticsItem(BaseModel):
    get: str = Field(exclude=True)
    parameters: Parameters
    errors: list
    results: int = Field(exclude=True)
    paging: Paging = Field(exclude=True)
    response: list[ResponseItem]


class Statistics(BaseModel):
    responses: list[StatisticsItem]
