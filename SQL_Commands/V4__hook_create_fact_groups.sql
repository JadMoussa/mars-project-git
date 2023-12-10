

CREATE TABLE IF NOT EXISTS dw_reporting. (
    country_code PRIMARY KEY NOT NULL,
    match_id INT,
    year INT,
    stage VARCHAR,
    city VARCHAR,
    attendence INT,
    home_team_id INT,
    away_team_id INT,
    home_team_goals INT,
    away_team_goals INT

);
Insert INTO dw_reporting.fact_groups
(
    country_code,
    match_id,
    year,
    stage,
    city,
    attendence,
    home_team_id,
    away_team_id,
    home_team_goals,
    away_team_goals
)
INSERT INTO dw_reporting.
SELECT 
 country_code,
    match_id,
    year,
    stage,
    city,
    attendence,
    home_team_id,
    away_team_id,
    home_team_goals,
    away_team_goals
     FROM dw_reporting.fact_groups




