from abc import ABC, abstractmethod
from typing import List

import backoff
import requests


class Requester(ABC):
    def __init__(self, api_host_key: str, api_secert_key: str):
        """Abstract base class for all API requesters.
        Work as interface for all API requesters, forcing to implement some methods.
        Also use an exteranl library for rate limiting and back-off exponential
        to timing out requests.

        Args:
            api_host_key: Api credentials host key
            api_secert_key: Api credentials secret key
        """
        self.headers = {
            "X-RapidAPI-Host": api_host_key,
            "X-RapidAPI-Key": api_secert_key,
        }
        self.base_endpoint = "https://api-football-v1.p.rapidapi.com/v3/"

    @abstractmethod
    def _get_endpoint(self) -> str:
        """Abstract method to generate the endpoint for the API"""
        raise NotImplementedError

    @abstractmethod
    def _get_params(self, **kwargs) -> dict | list[dict]:
        """Abstract method to generate the parameters for the API`
        Args:
            **kwargs: Some extra parameters for the API
        """
        raise NotImplementedError

    @backoff.on_exception(backoff.expo, requests.exceptions.HTTPError, max_tries=10, factor=10)
    def get_response(self, endpoint: str, header: str, param: dict) -> requests.Response:
        """Concrete method to get the response from the API. This request method use
        back-off exponential to timing out requests, with max retries and factor are set to 10.
        Also only retry if the response is not 2xx.

        Args:
            endpoint: Endpoint string
            header: Header for the request
            param: Parameter for the request

        Returns:
            Response from the API
        """
        print(f"Request {endpoint} with {param} as parameter")
        response = requests.request("GET", endpoint, headers=header, params=param)
        response.raise_for_status()
        return response

    def get_data(self, **kwargs) -> dict | list[dict]:
        """Method to get the data from the API. This methods warperd _get_response_ to set
        some parameters for the API.

        Args:
            **kwargs: Extra parameters for the API

        Returns:
            Json response from the API
        """
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
