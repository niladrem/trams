SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=non-strict;
SET hive.enforce.bucketing=true;

CREATE EXTERNAL TABLE trams (
    Lines STRING,
    Lon DOUBLE,
    Lat DOUBLE,
    VehicleNumber STRING,
    Brigade STRING,
    T TIMESTAMP
)
PARTITIONED BY (day STRING)
STORED AS PARQUET
LOCATION "/data/trams";

ALTER TABLE trams SET SERDEPROPERTIES ("timestamp.formats"="yyyy-MM-dd'T'HH:mm:ss,yyyy-MM-dd'T'HH:mm:ss.SSS");

