WITH game_info AS (
    SELECT 
        g.id,
        g.game_code,
        RIGHT(g.game_code, 4) AS season,
        CAST(LEFT(g.game_code, LENGTH(g.game_code) - 5) AS INTEGER) AS game_number,
        g.home_team_id,
        g.away_team_id,
        g.home_team_score,
        g.away_team_score,
        CASE 
            WHEN g.home_team_score > g.away_team_score THEN g.home_team_id 
            ELSE g.away_team_id 
        END AS winning_team_id
    FROM games g
),
player_box AS (
    SELECT 
        b.*,
        gi.season,
        gi.game_number,
        -- Determine the opponent team for this game
        CASE 
            WHEN gi.home_team_id = b.team_id THEN gi.away_team_id 
            ELSE gi.home_team_id 
        END AS opponent_team_id,
        -- Averages for valuation and other stats over the last 3 games (prior to current game)
        COALESCE(AVG(b.valuation) OVER (
            PARTITION BY b.player_id, gi.season 
            ORDER BY gi.game_number 
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ), 0) AS avg_last_3_valuation,
        COALESCE(AVG(b.seconds) OVER (
            PARTITION BY b.player_id, gi.season 
            ORDER BY gi.game_number 
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ), 0) AS avg_last_3_seconds,
        COALESCE(AVG(b.points) OVER (
            PARTITION BY b.player_id, gi.season 
            ORDER BY gi.game_number 
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ), 0) AS avg_last_3_points,
        COALESCE(AVG(b.fg_made_2) OVER (
            PARTITION BY b.player_id, gi.season 
            ORDER BY gi.game_number 
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ), 0) AS avg_last_3_fg_made_2,
        COALESCE(AVG(b.fg_attempted_2) OVER (
            PARTITION BY b.player_id, gi.season 
            ORDER BY gi.game_number 
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ), 0) AS avg_last_3_fg_attempted_2,
        COALESCE(AVG(b.fg_made_3) OVER (
            PARTITION BY b.player_id, gi.season 
            ORDER BY gi.game_number 
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ), 0) AS avg_last_3_fg_made_3,
        COALESCE(AVG(b.fg_attempted_3) OVER (
            PARTITION BY b.player_id, gi.season 
            ORDER BY gi.game_number 
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ), 0) AS avg_last_3_fg_attempted_3,
        COALESCE(AVG(b.ft_made) OVER (
            PARTITION BY b.player_id, gi.season 
            ORDER BY gi.game_number 
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ), 0) AS avg_last_3_ft_made,
        COALESCE(AVG(b.ft_attempted) OVER (
            PARTITION BY b.player_id, gi.season 
            ORDER BY gi.game_number 
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ), 0) AS avg_last_3_ft_attempted,
        COALESCE(AVG(b.offensive_rebounds) OVER (
            PARTITION BY b.player_id, gi.season 
            ORDER BY gi.game_number 
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ), 0) AS avg_last_3_offensive_rebounds,
        COALESCE(AVG(b.defensive_rebounds) OVER (
            PARTITION BY b.player_id, gi.season 
            ORDER BY gi.game_number 
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ), 0) AS avg_last_3_defensive_rebounds,
        COALESCE(AVG(b.total_rebounds) OVER (
            PARTITION BY b.player_id, gi.season 
            ORDER BY gi.game_number 
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ), 0) AS avg_last_3_total_rebounds,
        COALESCE(AVG(b.assists) OVER (
            PARTITION BY b.player_id, gi.season 
            ORDER BY gi.game_number 
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ), 0) AS avg_last_3_assists,
        COALESCE(AVG(b.steals) OVER (
            PARTITION BY b.player_id, gi.season 
            ORDER BY gi.game_number 
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ), 0) AS avg_last_3_steals,
        COALESCE(AVG(b.turnovers) OVER (
            PARTITION BY b.player_id, gi.season 
            ORDER BY gi.game_number 
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ), 0) AS avg_last_3_turnovers,
        COALESCE(AVG(b.blocks_favor) OVER (
            PARTITION BY b.player_id, gi.season 
            ORDER BY gi.game_number 
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ), 0) AS avg_last_3_blocks_favor,
        COALESCE(AVG(b.blocks_against) OVER (
            PARTITION BY b.player_id, gi.season 
            ORDER BY gi.game_number 
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ), 0) AS avg_last_3_blocks_against,
        COALESCE(AVG(b.fouls_committed) OVER (
            PARTITION BY b.player_id, gi.season 
            ORDER BY gi.game_number 
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ), 0) AS avg_last_3_fouls_committed,
        COALESCE(AVG(b.fouls_received) OVER (
            PARTITION BY b.player_id, gi.season 
            ORDER BY gi.game_number 
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ), 0) AS avg_last_3_fouls_received,
        COALESCE(AVG(b.plus_minus) OVER (
            PARTITION BY b.player_id, gi.season 
            ORDER BY gi.game_number 
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ), 0) AS avg_last_3_plus_minus,
        -- Valuation of the next game (target to predict)
        LEAD(b.valuation) OVER (
            PARTITION BY b.player_id, gi.season 
            ORDER BY gi.game_number
        ) AS next_game_valuation,
        -- Win indicator for the player's team in the current game
        CASE 
            WHEN (gi.home_team_id = b.team_id AND gi.home_team_score > gi.away_team_score)
              OR (gi.away_team_id = b.team_id AND gi.away_team_score > gi.home_team_score)
            THEN 1 
            ELSE 0 
        END AS win_ind
    FROM boxscore_record b
    JOIN game_info gi ON b.game_id = gi.id
),
team_games AS (
    -- For each game, generate two rows (one per team) and mark wins
    SELECT 
        gi.season,
        gi.game_number,
        team_id,
        CASE WHEN team_id = gi.winning_team_id THEN 1 ELSE 0 END as win
    FROM game_info gi
    CROSS JOIN LATERAL (VALUES (gi.home_team_id), (gi.away_team_id)) AS t(team_id)
),
cumulative_win AS (
    SELECT
        season,
        team_id,
        game_number,
        COALESCE(
          SUM(win) OVER (
              PARTITION BY team_id, season 
              ORDER BY game_number 
              ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
          ), 0) AS cum_wins,
        (ROW_NUMBER() OVER (PARTITION BY team_id, season ORDER BY game_number) - 1) AS games_played_before,
        CASE 
            WHEN (ROW_NUMBER() OVER (PARTITION BY team_id, season ORDER BY game_number) - 1) = 0 
            THEN 0
            ELSE COALESCE(
              SUM(win) OVER (
                  PARTITION BY team_id, season 
                  ORDER BY game_number 
                  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
              ), 0) / (ROW_NUMBER() OVER (PARTITION BY team_id, season ORDER BY game_number) - 1)::FLOAT
        END AS cum_win_ratio
    FROM team_games
)
SELECT 
    pb.*,
    ctw.cum_win_ratio AS team_win_ratio,
    ctopp.cum_win_ratio AS opponent_win_ratio,
    CASE 
       WHEN ctopp.cum_win_ratio = 0 THEN NULL
       ELSE ctw.cum_win_ratio / ctopp.cum_win_ratio
    END AS win_ratio_ratio
FROM player_box pb
LEFT JOIN cumulative_win ctw 
    ON pb.season = ctw.season 
    AND pb.team_id = ctw.team_id
    AND pb.game_number = ctw.game_number
LEFT JOIN cumulative_win ctopp 
    ON pb.season = ctopp.season 
    AND pb.opponent_team_id = ctopp.team_id 
    AND pb.game_number = ctopp.game_number
ORDER BY pb.game_id, pb.player_id;
