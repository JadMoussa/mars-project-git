

CREATE TABLE IF NOT EXISTS dw_reporting. (
    country_code FOREIGN KEY NOT NULL,
    team_name VARCHAR,
    team_id INT
);
INSERT INTO dw_reporting.dimension_teams
(
    country_code ,
    team_name ,
    team_id
)
SELECT
country_code ,
    team_name ,
    team_id
FROM dw_reporting.


