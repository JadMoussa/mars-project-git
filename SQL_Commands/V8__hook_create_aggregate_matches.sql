

CREATE TABLE IF NOT EXISTS dw_reporting. (
    country_code FOREIGN KEY NOT NULL,
    team_id INT,
    year INT,
    match_played INT,
    wins INT,
    losses INT,
    goals_scored INT,
    goals_conceded INT,
    attendence_avg FLOAT
);


