CREATE TABLE IF NOT EXISTS dw_reporting.dim_team
(
    team_id PRIMARY KEY NOT NULL,
    team_name VARCH 
);

INSERT INTO dw_reporting.dim_team 
(team_id, team_name)
SELECT 
    team_id ,
    team_name 
 FROM dw_reporting.

ON CONFLICT UPDATE fields
    -- 