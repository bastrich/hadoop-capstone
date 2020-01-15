package com.bastrich.etl.spark.rdd

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Runner extends App {
  val config = ConfigFactory.load()
  val spark = SparkSession
    .builder()
    .config(new SparkConf())
    .appName("Spark ETL Datasets")
    .getOrCreate()
  val sparkRunner = new SparkETL_RDD(spark, config)
  try {

    val dbConnector = new DbConnector(config)
    import dbConnector.DbWriteable

    sparkRunner.top10MostFrequentlyPurchasedCategories().withDbConnection((result, connection) => {
      result.foreach { category =>
        connection.createStatement().execute(s"INSERT INTO spark_rdd.top10_categories VALUES ('${category._1}', ${category._2})")
      }
    })

    sparkRunner.top10MostFrequentlyPurchasedProductsInEachCategory().withDbConnection((result, connection) => {
      result.foreach { category =>
        category._2.foreach { product =>
          connection.createStatement().execute(s"INSERT INTO spark_rdd.top10_products_within_categories " +
            s"VALUES ('${category._1}', '${product._1._1}', ${product._1._2}, ${product._2})")
        }
      }
    })

    sparkRunner.top10CountriesWithTheHighestMoneySpending().withDbConnection((result, connection) => {
      result.foreach { country =>
        connection.createStatement().execute(s"INSERT INTO spark_rdd.top10_countries VALUES ('${country._1}', ${country._2})")
      }
    })

  } catch {
    case e: Exception => throw e
  } finally {
    spark.close()
  }
}
