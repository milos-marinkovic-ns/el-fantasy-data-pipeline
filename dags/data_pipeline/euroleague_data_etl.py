import boto3
import json
from models.models import Player, Team, Game, BoxScoreRecord, session
from utils.db_utils import (PostgresqlTeamRepository,
                                PostgresqlPlayerRepository,
                                PostgresqlGameRepository,
                                PostgresqlBoxscoreRecordRepository)
import yaml
import os

class EuroleagueDataETL:

    last_transformed_config_key: str = "configs/last_data_transformed.yaml"
    
    def __init__(self):
        self.s3 = boto3.client('s3')

        self.team_repository = PostgresqlTeamRepository(session)
        self.player_repository = PostgresqlPlayerRepository(session)
        self.game_repository = PostgresqlGameRepository(session)
        self.boxscore_repository = PostgresqlBoxscoreRecordRepository(session)

        try:
            old_obj = self.s3.get_object(
                Bucket="euroleague-boxscore-data", 
                Key=self.last_transformed_config_key
            )
            self.last_transformed_config: dict = yaml.safe_load(old_obj['Body'].read())
        except Exception as e:
            raise Exception(f"Error loading old data config from S3: {e}")

    
    def list_all_json_files(self, bucket: str, prefix: str) -> list[str]:

        keys = []
        paginator = self.s3.get_paginator('list_objects_v2')

        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key.endswith('.json'):
                    keys.append(key)


        return keys    
    
    def check_if_game_invalid(self, data: dict):
        return data['Referees'] == "N/D, N/D, N/D"
    
    def convert_minutes_to_seconds(self, minutes_str):
        """
        Convert a minutes string to seconds.
        If the player did not play (e.g. "DNP"), return 0.
        """
        try:
            minutes = float(minutes_str.split(":")[0])
            return int(minutes * 60) + float(minutes_str.split(":")[1])
        except ValueError:
            return 0

    
    def get_game_code_from_path(self, key):
        key_sliced = str(key).split("/")
        return key_sliced[-1].split(".")[0] + key_sliced[-2] 
    
    def load_teams_to_db(self, data: dict) -> tuple[Team]:
        home_team = Team(name=data['Stats'][0]['tmr']['Team'])
        away_team = Team(name=data['Stats'][1]['tmr']['Team'])

        team_a = self.team_repository.add(home_team)
        team_b = self.team_repository.add(away_team)
        return team_a, team_b
    
    def load_players_to_db(self, data: dict) -> list[Player]:
        added_players = []
        players_data = data['Stats'][0]['PlayersStats'] + data['Stats'][1]['PlayersStats']
        for player_data in players_data:
            player = Player(player_code=player_data['Player_ID'].strip(), name=player_data['Player'])
            added_players.append(self.player_repository.add(player))
        return added_players
    
    def load_games_to_db(self, data: dict, game_code: str, home_team: Team, away_team: Team):
        home_team_score = name=data['Stats'][0]['totr']['Points']
        away_team_score = name=data['Stats'][1]['totr']['Points']

        game = Game(
            home_team=home_team,
            away_team=away_team,
            game_code=game_code,
            home_team_score=home_team_score,
            away_team_score=away_team_score
        )

        return self.game_repository.add(game)

    def load_boxscore_data_to_db(self, data: dict, game_code: str):
        team_h, team_a = self.load_teams_to_db(data)
        added_players = self.load_players_to_db(data)
        game = self.load_games_to_db(data, game_code, team_h, team_a)

        players_stats = data['Stats'][0]['PlayersStats'] + data['Stats'][1]['PlayersStats']

        for player_stat, added_player in zip(players_stats, added_players):

            team_name = player_stat["Team"].strip()
            played_for_home = team_name == team_h.name
            team_id = team_h.id if played_for_home else team_a.id

            box_score = BoxScoreRecord(
                player_id=added_player.id,  
                game_id=game.id,            
                team_id=team_id,         
                played_for_home=played_for_home,
                is_starter=bool(player_stat["IsStarter"]),
                seconds=self.convert_minutes_to_seconds(player_stat["Minutes"]),
                points=player_stat["Points"],
                fg_made_2=player_stat["FieldGoalsMade2"],
                fg_attempted_2=player_stat["FieldGoalsAttempted2"],
                fg_made_3=player_stat["FieldGoalsMade3"],
                fg_attempted_3=player_stat["FieldGoalsAttempted3"],
                ft_made=player_stat["FreeThrowsMade"],
                ft_attempted=player_stat["FreeThrowsAttempted"],
                offensive_rebounds=player_stat["OffensiveRebounds"],
                defensive_rebounds=player_stat["DefensiveRebounds"],
                total_rebounds=player_stat["TotalRebounds"],
                assists=player_stat["Assistances"],
                steals=player_stat["Steals"],
                turnovers=player_stat["Turnovers"],
                blocks_favor=player_stat["BlocksFavour"],
                blocks_against=player_stat["BlocksAgainst"],
                fouls_committed=player_stat["FoulsCommited"],
                fouls_received=player_stat["FoulsReceived"],
                valuation=player_stat["Valuation"],
                plus_minus=player_stat["Plusminus"]
            )
            
            self.boxscore_repository.add(box_score)

    def process_key(self, bucket:str, key: str):
        obj = self.s3.get_object(Bucket=bucket, Key=key)
        data = json.loads(obj['Body'].read())

        if self.check_if_game_invalid(data):
            return
        
        game_code = self.get_game_code_from_path(key)

        self.load_boxscore_data_to_db(data, game_code)

    def filter_files(self, file_list, target_year, threshold):
        filtered_files = []
        for file in file_list:
            norm_path = os.path.normpath(file)
            parts = norm_path.split(os.sep)
            year_index = 2
            file_year = parts[year_index]
            file_number = int(parts[year_index + 1].replace('.json', ''))
            if file_year == target_year and file_number > int(threshold):
                filtered_files.append(file)
        return filtered_files


    def get_max_file_number_from_year(self, target_year, bucket, prefix):
        max_val = None
        for file in self.list_all_json_files(bucket=bucket, prefix=prefix):
            parts = file.split('/')
            
            file_year = parts[2]
            if file_year != target_year:
                continue
            try:
                file_number = int(parts[3].replace('.json', ''))
            except ValueError:
                continue
            
            if max_val is None or file_number > max_val:
                max_val = file_number
        return max_val


    def update_data_config(self, current_season: str, bucket: str, prefix: str) -> bool:

        try:
            obj = self.s3.get_object(
                Bucket=bucket, 
                Key=self.last_transformed_config_key
            )
            config: dict = yaml.safe_load(obj['Body'].read())

            config['last_transformed_id'] = str(self.get_max_file_number_from_year(current_season, bucket, prefix))

            new_yaml = yaml.dump(config, default_flow_style=False)
            self.s3.put_object(
                Bucket=bucket, 
                Key=self.last_transformed_config_key, 
                Body=new_yaml, 
                ContentType='application/x-yaml'
            )
            return True
        except Exception as e:
            print(f"Error updating data config on S3: {e}")
            return False


    def transform_new_box_score_data(self, bucket: str, prefix: str):

        all_json_files: str = self.list_all_json_files(bucket, prefix)
        current_season: str = self.last_transformed_config['current_season']
        last_transformed_id: str = self.last_transformed_config['last_transformed_id']


        for key in self.filter_files(all_json_files, current_season, last_transformed_id):
            print(f"Transforming .json with key={key}...")
            self.process_key(bucket=bucket, key=key)

        self.update_data_config(current_season, bucket, prefix)


    def transform_old_box_score_data(self, bucket: str, prefix: str):

        for key in self.list_all_json_files(bucket, prefix):
            print(f"Transforming .json with key={key}...")
            self.process_key(bucket=bucket, key=key)


    