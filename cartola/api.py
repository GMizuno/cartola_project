import logging
from abc import abstractmethod
from typing import List
import backoff
import ratelimit

import requests

my_logger = logging.getLogger('my_logger')
my_handler = logging.StreamHandler()
my_logger.addHandler(my_handler)
my_logger.setLevel(logging.ERROR)


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

    @backoff.on_exception(backoff.expo, ratelimit.exception.RateLimitException, logger=my_logger, max_tries=10, factor=10)
    @backoff.on_exception(backoff.expo, requests.exceptions.HTTPError, logger=my_logger, max_tries=5, factor=10)
    def get_response(self, endpoint, header, param):  
        response = requests.request("GET", endpoint, headers=header, params=param)
        response.raise_for_status()
        return response

    def get_data(self, **kwargs):
        endpoint = self._get_endpoint()
        params = self._get_params(**kwargs)
        responses_json = [self.get_response(endpoint, self.headers, param).json() for param in params]
        return responses_json


class Fixtures(Requester):
    def _get_endpoint(self) -> str:
        return f'{self.base_endpoint}fixtures'

    def _get_params(self, season_year: str, league_id: str) -> List[dict]:
        return [{"league": league_id, "season": season_year}]


class Teams(Requester):
    def _get_endpoint(self) -> str:
        return f'{self.base_endpoint}teams'

    def _get_params(self, team_id: List[str]) -> List[dict]:
        return [{"id": id} for id in team_id]


class Matches(Requester):
    def _get_endpoint(self) -> str:
        return f'{self.base_endpoint}fixtures/statistics'

    def _get_params(self, match_id: List[str]) -> List[dict]:
        return [{"fixture": id} for id in match_id]
