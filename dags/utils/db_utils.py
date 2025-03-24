from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from models.models import Player, Team, Game, BoxScoreRecord

class PostgresqlPlayerRepository:
    
    def __init__(self, db_session: Session):
        self.db_session = db_session

    def add(self, player: Player) -> Player:
        try:
            self.db_session.add(player)
            self.db_session.commit()
            self.db_session.refresh(player)
            return player
        except IntegrityError:
            self.db_session.rollback()
            existing_player = self.db_session.query(Player).filter_by(player_code=player.player_code).first()
            return existing_player


    def delete(self, player_id: int):
        player = self.db_session.query(Player).filter(Player.id == player_id).first()
        if player:
            self.db_session.delete(player)
            self.db_session.commit()

    def get_by_id(self, player_id: int) -> Player:
        return self.db_session.query(Player).filter(Player.id == player_id).first()

    def get_all(self):
        return self.db_session.query(Player).all()
    

class PostgresqlTeamRepository:

    def __init__(self, db_session: Session):
        self.db_session = db_session

    def add(self, team: Team) -> Team:
        try:
            self.db_session.add(team)
            self.db_session.commit()
            self.db_session.refresh(team)
            return team
        except IntegrityError:
            self.db_session.rollback()
            existing_team = self.db_session.query(Team).filter_by(name=team.name).first()
            return existing_team


    def delete(self, team_id: int):
        team = self.db_session.query(Team).filter(Team.id == team_id).first()
        if team:
            self.db_session.delete(team)
            self.db_session.commit()

    def get_by_id(self, team_id: int) -> Team:
        return self.db_session.query(Team).filter(Team.id == team_id).first()

    def get_all(self):
        return self.db_session.query(Team).all()


class PostgresqlGameRepository:

    def __init__(self, db_session: Session):
        self.db_session = db_session

    def add(self, game: Game) -> Game:
        try:
            self.db_session.add(game)
            self.db_session.commit()
            self.db_session.refresh(game)
            return game
        except IntegrityError:
            self.db_session.rollback()
            existing_game = self.db_session.query(Game).filter_by(game_code=game.game_code).first()
            return existing_game

    def delete(self, game_id: int):
        game = self.db_session.query(Game).filter(Game.id == game_id).first()
        if game:
            self.db_session.delete(game)
            self.db_session.commit()

    def get_by_id(self, game_id: int) -> Game:
        return self.db_session.query(Game).filter(Game.id == game_id).first()

    def get_all(self):
        return self.db_session.query(Game).all()

class PostgresqlBoxscoreRecordRepository:
    
    def __init__(self, db_session: Session):
        self.db_session = db_session

    def add(self, box_score: BoxScoreRecord) -> BoxScoreRecord:
        try:
            self.db_session.add(box_score)
            self.db_session.commit()
            self.db_session.refresh(box_score)
            return box_score
        except IntegrityError:
            self.db_session.rollback()
            return self.db_session.query(BoxScoreRecord).filter_by(
                player_id=box_score.player_id, game_id=box_score.game_id
            ).first()

    def delete(self, boxscore_id: int):
        boxscore = self.db_session.query(BoxScoreRecord).filter(BoxScoreRecord.id == boxscore_id).first()
        if boxscore:
            self.db_session.delete(boxscore)
            self.db_session.commit()

    def get_by_id(self, boxscore_id: int) -> BoxScoreRecord:
        return self.db_session.query(BoxScoreRecord).filter(BoxScoreRecord.id == boxscore_id).first()

    def get_all(self):
        return self.db_session.query(BoxScoreRecord).all()