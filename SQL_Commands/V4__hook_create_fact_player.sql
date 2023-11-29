

CREATE TABLE IF NOT EXISTS dw_reporting.player (
    player_id INTEGER,
    player_name TEXT,
    country_code PRIMARY KEY NOT NULL,
    coach_name TEXT
);

