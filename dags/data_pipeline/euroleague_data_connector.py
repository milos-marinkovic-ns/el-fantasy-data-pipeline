from dataclasses import dataclass
from typing import Any, Optional, List
import requests
from time import sleep
import boto3
from botocore.exceptions import BotoCoreError, ClientError
import yaml
import json
import os

base_dir = os.path.dirname(__file__)

@dataclass
class FetchResult:

    success: bool
    data: Optional[Any] = None
    error: Optional[str] = None


class EuroleagueDataConnector:

    boxscore_s3_bucket_name: str = "euroleague-boxscore-data" 
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
            

    def fetch_old_box_score_data(self) -> List[FetchResult]:
        """
        Fetch all old box score data for each season and game ID.
        Returns a list of FetchResult objects.
        """
        results: List[FetchResult] = []
        for season_code, max_game_id in self.seasons_games:
            for game_id in range(1, int(max_game_id) + 1):
                result = self.fetch_box_score_data(game_id, season_code)
                if result.success:
                    if self.save_json_to_s3(result.data, self.boxscore_s3_bucket_name, f"data/boxscore_json/{season_code}/{game_id}.json"):
                        print(f"Fetched and saved data for season: {season_code} game ID: {game_id}")
                    else:
                        result = FetchResult(success=False, error=f"Failed to save JSON for season: {season_code} game ID: {game_id}")
                else:
                    print(result.error)
                results.append(result)
        return results
    
    def update_data_config(self, current_season: Optional[str] = None, last_fetched_id: Optional[int] = None) -> bool:
        """
        Update the new box score YAML config file with provided parameters.
        If parameters are not provided, keep existing values.
        """
        try:
            # Load the existing config
            with open(self.new_box_score_config_path, "r") as file:
                config: dict = yaml.safe_load(file)

            if current_season is not None:
                config['current_season'] = current_season
            if last_fetched_id is not None:
                config['last_fetched_id'] = str(last_fetched_id)

            with open(self.new_box_score_config_path, "w") as file:
                yaml.dump(config, file, default_flow_style=False)
            return True
        except Exception as e:
            print(f"Error updating data config: {e}")
            return False
    
    def fetch_new_box_score_data(self) -> List[FetchResult]:
        """
        Continuously fetch new box score data for the current season starting from last_fetched_game_id.
        Returns a list of FetchResult objects for each fetch attempt.
        """
        results: List[FetchResult] = []
        while True:
            result = self.fetch_box_score_data(self.last_fetched_game_id, self.current_season_code)
            if result.success:
                if self.save_json_to_s3(result.data, self.boxscore_s3_bucket_name, f"data/boxscore_json/{self.current_season_code}/{self.last_fetched_game_id}.json"):
                    print(f"Fetched and saved data for season: {self.current_season_code} game ID: {self.last_fetched_game_id}")
                else:
                    result = FetchResult(success=False,
                                        error=f"Failed to save JSON for season: {self.current_season_code} game ID: {self.last_fetched_game_id}")
                    results.append(result)
                    break
            else:
                print(f"Stopping fetch_new_box_score_data due to error: {result.error}")
                results.append(result)
                break
            results.append(result)
            self.last_fetched_game_id += 1

        if self.update_data_config(last_fetched_id=self.last_fetched_game_id):
            print("Data config updated successfully.")
        else:
            print("Failed to update data config.")

        return results

    def save_json_to_s3(self, data: dict, bucket: str, key: str) -> bool:
        """
        Saves dictionary data in .json format into s3 bucket
        """

        s3 = boto3.client('s3')

        try:
            json_data = json.dumps(data, indent=4)
            s3.put_object(Bucket=bucket, Key=key, Body=json_data, ContentType='application/json')
            return True
        except (BotoCoreError, ClientError, Exception) as e:
            print(f"Error saving JSON to s3://{bucket}/{key}: {e}")
            return False

