#!/bin/bash -e

# $1 - flume.properties location
# $2 - data generator jar location
# $3 - location of csv-file with ip_block -> country_code
# $4 - location of csv-file with country_code -> country_name
# $5 - location of hive sql script with tables creation
# $6 - start date for generated events (YYYY-MM-DD)
# $7 - end date for generated events (YYYY-MM-DD)
# $8 - path to jar with hive udf
# $9 - sqoop options file for top 10 categories
# $10 - sqoop options file for top 10 products within categories
# $11 - sqoop options file for top 10 countries

# prepare to collect data
rm -rf ~/.flume
nohup flume-ng agent -n agent --conf /etc/flume/conf -f "$1" -Dflume.root.logger=INFO,console &

sleep 5

# generate data
java -cp "$2" -Dgenerator.target.date.from="2019-01-01" -Dgenerator.target.date.to="2020-01-01" -Dgenerator.target.events.number=1000 com.bastrich.DataGenerator

# prepare hive tables
hdfs dfs -mkdir -p /geo_data/ip_blocks
hdfs dfs -mkdir -p /geo_data/countries
hdfs dfs -put "$3" /geo_data/ip_blocks/ip_blocks.csv
hdfs dfs -put "$4" /geo_data/countries/countries.csv

hdfs dfs -put "$8" /apps/hive-udf.jar
hive -f "$5"

# add partitions to "events" hive table
rm -rf /tmp/hive_add_partitions.sql
dateDashed=$6
dateSlashed=$(date -d "$dateDashed" +%Y/%m/%d)
upperDateExclusive=$(date -I -d "$7 + 1 day")
while [ "$dateDashed" != "$upperDateExclusive" ]; do
  echo "ALTER TABLE events ADD PARTITION (purchase_date='$dateDashed') LOCATION '/events/$dateSlashed';" >> /tmp/hive_add_partitions.sql
  dateDashed=$(date -I -d "$dateDashed + 1 day")
  dateSlashed=$(date -d $dateDashed +%Y/%m/%d)
done
hive -f /tmp/hive_add_partitions.sql
rm -rf /tmp/hive_add_partitions.sql

# prepare RDBMS for saving results
psql -f rdbms_prep.sql

# execute hive selects
hive -f hive_selects.sql

# export hive results to rdbms
sqoop export --options-file "$9"
sqoop export --options-file "${10}"
sqoop export --options-file "${11}"

# run spark rdd etl
spark-submit --class com.bastrich.etl.spark.rdd.Runner --master yarn spark-etl.jar

# run spark datasets etl
spark-submit --class com.bastrich.etl.spark.datasets.Runner --master yarn spark-etl.jar