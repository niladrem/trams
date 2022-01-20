#!/bin/bash

# katalogi na tabele externalne i dump danych
hdfs dfs -mkdir /data
hdfs dfs -mkdir /data/in
hdfs dfs -mkdir /data/trams
hdfs dfs -mkdir /data/stops

# stworzenie tabel (skrypt w extra)
hive -f create_table.hql

# certy do API
keytool -import -alias nificert -file dvcasha2.cer -keystore truststore.jks -storepass $PASS

# stworzenie topiców
/usr/local/kafka/bin/kafka-topics.sh --create --topic input-trams --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
/usr/local/kafka/bin/kafka-topics.sh --create --topic output-trams --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
/usr/local/kafka/bin/kafka-topics.sh --create --topic fast-trams --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# deploy joba flinkowego (config w extra)
sudo /usr/local/flink/bin/start-cluster.sh
sudo /usr/local/flink/bin/flink run -c pw.mini.DeduplicationJob /home/vagrant/trams/deduplication-1.0-SNAPSHOT.jar /home/vagrant/trams/config.properties

# thrift do skryptów z użyciem happybase
hbase thrift -p 9090

# tworzenie tabel w hbase shell
create 'tramsStats', 'infoTeam', 'infoTram', 'stats'
create 'linesStats', 'dayStats', 'stats'
