CREATE EXTERNAL TABLE IF NOT EXISTS ` cartola_siver `.` matches ` (
    ` partida_id ` int,
    ` date ` string,
    ` reference_date ` string,
    ` rodada ` string,
    ` id_team_away ` int,
    ` id_team_home ` int,
    ` league_id ` int
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
         LOCATION 's3://bootcamp-silver/matches'
TBLPROPERTIES ('has_encrypted_data' = 'false');

CREATE EXTERNAL TABLE IF NOT EXISTS ` cartola_siver `.` teams ` (
    ` team_id ` string,
    ` name ` string,
    ` code ` string,
    ` city ` string,
    ` logo ` string
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
         LOCATION 's3://bootcamp-silver/teams/'
TBLPROPERTIES ('has_encrypted_data' = 'false');

CREATE EXTERNAL TABLE IF NOT EXISTS ` cartola_siver `.` statistics ` (
    Shots_on_Goal string
    Shots_off_Goal string
    Total_Shots string
    Blocked_Shots string
    Shots_insidebox string
    Shots_outsidebox string
    Fouls string
    Corner_Kicks string
    Offsides string
    Ball_Possession string
    Yellow_Cards string
    Red_Cards string
    Goalkeeper_Saves string
    Total_passes string
    Passes_accurate string
    Passes_percentage string
    team_id string
    match_id string

    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('serialization.format' = '1')
         LOCATION 's3://bootcamp-silver/statistics/'
TBLPROPERTIES ('has_encrypted_data' = 'false');

DROP TABLE ` matches `;
DROP TABLE ` teams `;
DROP TABLE ` statistics `;

SELECT *
FROM "cartola_siver"."matches";
SELECT *
FROM "cartola_siver"."teams";
SELECT *
FROM "cartola_siver"."statistics";