from abc import ABC, abstractmethod
from typing import List

import backoff
import ratelimit
import requests


class Requester(ABC):
    def __init__(self, api_host_key, api_secert_key):
        self.headers = {
            "X-RapidAPI-Host": api_host_key,
            "X-RapidAPI-Key": api_secert_key,
        }
        self.base_endpoint = "https://api-football-v1.p.rapidapi.com/v3/"

    @abstractmethod
    def _get_endpoint(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def _get_params(self, **kwargs) -> dict:
        raise NotImplementedError

    @backoff.on_exception(
        backoff.expo,
        ratelimit.exception.RateLimitException,
        max_tries=10,
        factor=10,
    )
    @backoff.on_exception(backoff.expo, requests.exceptions.HTTPError, max_tries=10, factor=10)
    def get_response(self, endpoint, header, param):
        print(f"Request {endpoint} with {param} as parameter")
        response = requests.request("GET", endpoint, headers=header, params=param)
        response.raise_for_status()
        return response

    def get_data(self, **kwargs) -> list:
        endpoint = self._get_endpoint()
        params = self._get_params(**kwargs)
        print(f"Using endpoint {endpoint} with {len(params)} parameter(s)")
        responses_json = [
            self.get_response(endpoint, self.headers, param).json() for param in params
        ]
        return responses_json


class Fixtures(Requester):
    def _get_endpoint(self) -> str:
        """Generates fixture`s  the endpoint for the API
        Returns:
            Endpoint string
        """
        return f"{self.base_endpoint}fixtures"

    def _get_params(self, season_year: str, league_id: str) -> List[dict]:
        """Generate a list of parameters for the API

        Args:
            season_year: Season year (in european format this year could different)
            league_id: Id of the league

        Returns:
            List containing a dict of the parameters league and season
        """
        return [{"league": league_id, "season": season_year}]


class Teams(Requester):
    def _get_endpoint(self) -> str:
        """Generates the team`s endpoint for the API
        Returns:
            Endpoint string
        """
        return f"{self.base_endpoint}teams"

    def _get_params(self, team_id: List[str]) -> List[dict]:
        """Generate a list of parameters for the API

        Args:
            team_id: List of team id

        Returns:
            List containing a dict of the parameters id that reporesents a team
        """
        return [{"id": id} for id in team_id]


class Matches(Requester):
    def _get_endpoint(self) -> str:
        """Generates the matches`s endpoint for the API
        Returns:
            Endpoint string
        """
        return f"{self.base_endpoint}fixtures/statistics"

    def _get_params(self, match_id: List[str]) -> List[dict]:
        """Generate a list of parameters for the API

        Args:
            match_id: List of match id

        Returns:
            List containing a dict of the parameters fixture that reporesents a match
        """
        return [{"fixture": id} for id in match_id]


class Players(Requester):
    def _get_endpoint(self) -> str:
        """Generates the player`s endpoint for the API
        Returns:
            Endpoint string
        """
        return f"{self.base_endpoint}fixtures/players"

    def _get_params(self, match_id: List[str]) -> List[dict]:
        """Generate a list of parameters for the API.
        This endpoint is used to get the players of a match.

        Args:
            match_id: List of match id

        Returns:
            List containing a dict of the parameters fixture that represents a match
        """
        return [{"fixture": id} for id in match_id]
