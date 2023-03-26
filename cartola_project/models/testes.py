from typing import Any
from dataclasses import dataclass
import json

@dataclass
class Team:
    id: int
    name: str
    code: str
    country: str
    founded: int
    national: bool
    logo: str

    @staticmethod
    def from_dict(obj: Any) -> 'Team':
        _id = int(obj.get("id"))
        _name = str(obj.get("name"))
        _code = str(obj.get("code"))
        _country = str(obj.get("country"))
        _founded = int(obj.get("founded"))
        _national = obj.get("national")
        _logo = str(obj.get("logo"))
        return Team(_id, _name, _code, _country, _founded, _national, _logo)

@dataclass
class Venue:
    id: int
    name: str
    address: str
    city: str
    capacity: int
    surface: str
    image: str

    @staticmethod
    def from_dict(obj: Any) -> 'Venue':
        _id = int(obj.get("id"))
        _name = str(obj.get("name"))
        _address = str(obj.get("address"))
        _city = str(obj.get("city"))
        _capacity = int(obj.get("capacity"))
        _surface = str(obj.get("surface"))
        _image = str(obj.get("image"))
        return Venue(_id, _name, _address, _city, _capacity, _surface, _image)

@dataclass
class Teams:
    team: Team
    venue: Venue

    @staticmethod
    def from_dict(obj: Any) -> 'Club':
        _team = Team.from_dict(obj.get("team"))
        _venue = Venue.from_dict(obj.get("venue"))
        return Teams(_team, _venue)



with open('cartola_project/models/teams.json', 'r') as f:
    dados = json.load(f)

json = dados[0].get('response')[0]
teams = Teams.from_dict(json)

