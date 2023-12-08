

CREATE TABLE IF NOT EXISTS dw_reporting. (
    country_code FOREIGN KEY NOT NULL,
    player_name VARCHAR,
    player_id INT,
    coach_name VARCHAR
);


