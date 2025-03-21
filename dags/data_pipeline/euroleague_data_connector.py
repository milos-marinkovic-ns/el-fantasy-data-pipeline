from dataclasses import dataclass
from typing import Any, Optional, List
import requests
from time import sleep
import boto3
import yaml
import os

base_dir = os.path.dirname(__file__)

@dataclass
class FetchResult:

    success: bool
    data: Optional[Any] = None
    error: Optional[str] = None


class EuroleagueDataConnector:

    old_box_score_config_path: str = os.path.join(base_dir, "configs", "old_box_score_data_config.yaml")
    new_box_score_config_path: str = os.path.join(base_dir, "configs", "new_box_score_data_config.yaml")

    def __init__(self):
        # Load data old config
        try:
            with open(self.old_box_score_config_path, "r") as file:
                old_box_score_config: dict = yaml.safe_load(file)
        except Exception as e:
            raise Exception(f"Error loading old data config: {e}")
        
        # Load data new config
        try:
            with open(self.new_box_score_config_path, "r") as file:
                new_box_score_config: dict = yaml.safe_load(file)
        except Exception as e:
            raise Exception(f"Error loading new data config: {e}")
        
        self.box_score_base_url: str = old_box_score_config["euroleague_api_config"]["box_score_base_url"]
        self.seasons_games: list[list[str]] = old_box_score_config["euroleague_api_config"]["box_score_queries"]

        self.current_season_code: str = new_box_score_config['current_season']
        self.last_fetched_game_id: int = int(new_box_score_config['last_fetched_id'])

    def fetch_box_score_data(self, game_id: int, season_code: str) -> FetchResult:
        """
        Fetch box score data from the API and return a FetchResult.
        """
        url: str = f"{self.box_score_base_url}?gamecode={game_id}&seasoncode={season_code}"
        try:
            res: requests.Response = requests.get(url, timeout=10)
            if res.status_code == 200:
                try:
                    json_data = res.json()
                    return FetchResult(success=True, data=json_data)
                except Exception as json_err:
                    return FetchResult(success=False, error=f"JSON decode error: {json_err}")
            else:
                return FetchResult(success=False, error=f"Failed to fetch data for season: {season_code} game ID: {game_id} - Status: {res.status_code}")
        except Exception as e:
            return FetchResult(success=False, error=str(e))
        finally:
            sleep(0.01)

    def dump_to_s3(self, data: dict) -> bool:
        print("Saving to s3 bucket...")