

CREATE TABLE IF NOT EXISTS dw_reporting. (
    country_code FOREIGN KEY NOT NULL,
    winner_team_id INT ,
    year INT ,     
    attendence INT,
    goals_scored INT,
    qualifying_teams VARCHAR,
    match_played INT
);

INSERT INTO dw_reporting.fact_winners
(
    country_code ,
    winner_team_id ,
    year  ,     
    attendence,
    goals_scored,
    qualifying_teams ,
    match_played 
)
SELECT 
country_code ,
    winner_team_id ,
    year  ,     
    attendence,
    goals_scored,
    qualifying_teams ,
    match_played 
    FROM dw_reporting.fact_winners

