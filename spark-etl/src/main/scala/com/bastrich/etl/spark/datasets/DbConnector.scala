package com.bastrich.etl.spark.datasets

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

class DbConnector(val config: Config) {

  implicit class DbWriteableDataFrame(val value: DataFrame) {
     def writeToDb(table: String): Unit = {
      value
        .write
        .format("jdbc")
        .option("url", config.getString("capstone.spark.etl.output.jdbc.url"))
        .option("dbtable", s"spark_datasets.$table")
        .option("user", config.getString("capstone.spark.etl.output.jdbc.user"))
        .option("password", config.getString("capstone.spark.etl.output.jdbc.password"))
        .save()
    }
  }

}

