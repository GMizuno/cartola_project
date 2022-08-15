from abc import abstractmethod
from typing import List

import requests


class Requester():
    def __init__(self, api_host_key, api_secert_key):
        self.headers = {
            "X-RapidAPI-Host": api_host_key,
            "X-RapidAPI-Key": api_secert_key
        }
        self.base_endpoint = 'https://api-football-v1.p.rapidapi.com/v3/'

    @abstractmethod
    def _get_endpoint(self) -> str:
        pass

    @abstractmethod
    def _get_params(self, **kwargs) -> dict:
        pass

    def get_data(self, **kwargs):
        endpoint = self._get_endpoint()
        params = self._get_params(**kwargs)
        response = requests.request("GET", endpoint, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()


class Fixtures(Requester):
    def _get_endpoint(self) -> str:
        return f'{self.base_endpoint}fixtures'

    def _get_params(self, season_year: str, league_id: str) -> dict:
        return {"league": league_id, "season": season_year}


class Teams(Requester):
    def _get_endpoint(self) -> str:
        return f'{self.base_endpoint}teams'

    def _get_params(self, team_id: List[str]) -> dict:
        return {"id": team_id}


class Matches(Requester):
    def _get_endpoint(self) -> str:
        return f'{self.base_endpoint}fixtures/statistics'

    def _get_params(self, match_id: List[str]) -> dict:
        return {"fixture": match_id}
