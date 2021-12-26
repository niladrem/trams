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
