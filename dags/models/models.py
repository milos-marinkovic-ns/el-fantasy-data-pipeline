from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Boolean, UniqueConstraint
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Get database connection URL from .env
DATABASE_URL = os.getenv('AWS_POSTGRESQL_DB_URL')
if not DATABASE_URL:
    raise ValueError("Missing AWS_POSTGRESQL_DB_URL environment variable!")
# Create database engine
engine = create_engine(DATABASE_URL, echo=True)

# Create session factory
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
session = SessionLocal()

# Base class for models
Base = declarative_base()


class Team(Base):
    """Represents a EuroLeague team."""
    __tablename__ = "teams"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, nullable=False)


class Player(Base):
    """Represents a player in a EuroLeague game."""
    __tablename__ = "players"

    id = Column(Integer, primary_key=True, index=True)
    player_code = Column(String, unique=True, nullable=False)
    name = Column(String, nullable=False)


class Game(Base):
    """Represents a EuroLeague game."""
    __tablename__ = "games"

    id = Column(Integer, primary_key=True, index=True)
    home_team_id = Column(Integer, ForeignKey("teams.id"), nullable=False)
    away_team_id = Column(Integer, ForeignKey("teams.id"), nullable=False)
    game_code = Column(String, unique=True, nullable=False)

    home_team_score = Column(Integer, default=0)
    away_team_score = Column(Integer, default=0)

    home_team = relationship("Team", foreign_keys=[home_team_id])
    away_team = relationship("Team", foreign_keys=[away_team_id])

    box_scores = relationship("BoxScoreRecord", back_populates="game")


class BoxScoreRecord(Base):
    """Represents a player's performance in a game."""
    __tablename__ = "boxscore_record"
    
    id = Column(Integer, primary_key=True, index=True)
    player_id = Column(Integer, ForeignKey("players.id"), nullable=False)
    game_id = Column(Integer, ForeignKey("games.id"), nullable=False)
    team_id = Column(Integer, ForeignKey("teams.id"), nullable=False)
    played_for_home = Column(Boolean, default=True)

    is_starter = Column(Boolean, default=False)
    seconds = Column(Integer, nullable=False)
    points = Column(Integer, default=0)
    
    fg_made_2 = Column(Integer, default=0)
    fg_attempted_2 = Column(Integer, default=0)
    fg_made_3 = Column(Integer, default=0)
    fg_attempted_3 = Column(Integer, default=0)
    
    ft_made = Column(Integer, default=0)
    ft_attempted = Column(Integer, default=0)
    
    offensive_rebounds = Column(Integer, default=0)
    defensive_rebounds = Column(Integer, default=0)
    total_rebounds = Column(Integer, default=0)
    
    assists = Column(Integer, default=0)
    steals = Column(Integer, default=0)
    turnovers = Column(Integer, default=0)
    
    blocks_favor = Column(Integer, default=0)
    blocks_against = Column(Integer, default=0)
    
    fouls_committed = Column(Integer, default=0)
    fouls_received = Column(Integer, default=0)
    
    valuation = Column(Integer, default=0)
    plus_minus = Column(Integer, default=0)  

    player = relationship("Player")
    game = relationship("Game", back_populates="box_scores")
    team = relationship("Team")

    __table_args__ = (UniqueConstraint("player_id", "game_id", name="unique_player_game"),)


