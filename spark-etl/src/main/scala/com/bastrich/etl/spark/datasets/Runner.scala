package com.bastrich.etl.spark.datasets

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
  val sparkRunner = new SparkETL_Datasets(spark, config)
  try {
    val dbConnector = new DbConnector(config)
    import dbConnector.DbWriteableDataFrame

    sparkRunner.top10MostFrequqentlyPurchasedCategories().writeToDb("top10_categories")
    sparkRunner.top10MostFrequentlyPurchasedProductsInEachCategory().writeToDb("top10_products_within_categories")
    sparkRunner.top10CountriesWithTheHighestMoneySpending().writeToDb("top10_countries")

  } catch {
    case e: Exception => throw e
  } finally {
    spark.close()
  }
}
