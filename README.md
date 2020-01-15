### **The whole pipeline can be found in /run_project.sh**

1. **data-generator** <br>
   build: _cd data-generator && sbt assembly_<br>
   run: _java -cp <jar_location> -Dgenerator.target.date.from="2019-01-01" -Dgenerator.target.date.to="2020-01-01" -Dgenerator.target.events.number=1000 com.bastrich.DataGenerator_
   (there are other config you can find in data-generator/src/main/resources/reference.conf)
2. **data-ingestion** <br>
   a) flume configs <br>
   b) geo data for the 3rd task<br>
   c) sql-scripts to prepare rdbms for saving final results
3. **hive-sqoop-etl** <br>
   a) hive sql scripts to init Hive with tables and to get final results<br>
   b) hive-udf - project with Hive UDF for the 3rd task (build: cd hive-sqoop-etl/hive-udf && sbt assembly)<br>
   c) sqoop properties to export results to rdbms<br>
4. **spark-etl** - both Spark RDD and Spark Datasets implemetations of needed solution<br>
   build: _spark-etl && sbt assembly_<br>
   run: _spark-submit --class com.bastrich.etl.spark.rdd.Runner --master yarn spark-etl.jar or spark-submit --class com.bastrich.etl.spark.datasets.Runner --master yarn spark-etl.jar_
   (there are other config you can find in spark-etl/src/main/resources/reference.conf)
5. **final_check.sql** - sql-script that checks tables equality in rdbms