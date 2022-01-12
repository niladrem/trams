SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=non-strict;
SET hive.enforce.bucketing=true;

CREATE EXTERNAL TABLE trams (
    Lines STRING,
    Lon DOUBLE,
    Lat DOUBLE,
    VehicleNumber STRING,
    Brigade STRING,
    Time BIGINT
)
PARTITIONED BY (day STRING)
STORED AS PARQUET
LOCATION "/data/trams";

CREATE EXTERNAL TABLE stops (
    zespol STRING,
    slupek STRING,
    nazwa_zespolu STRING,
    id_ulicy STRING,
    szer_geo DOUBLE,
    dlug_geo DOUBLE,
    kierunek STRING,
    obowiazuje_od STRING
)
STORED AS AVRO
LOCATION "/data/stops";
