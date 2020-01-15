package com.bastrich.etl.spark.datasets

import com.bastrich.etl.spark.utils
import com.typesafe.config.Config
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

class SparkETL_Datasets( val spark: SparkSession,
                         config: Config
                       ) {
  import spark.implicits._

  private val events = spark.read.format("csv")
    .schema(Encoders.product[Event].schema)
    .option("header", "false")
    .option("inferSchema", "true")
    .load(config.getString("capstone.spark.etl.input.events"))
    .as[Event]

  {
    Class.forName("org.postgresql.Driver")
  }

  def top10MostFrequqentlyPurchasedCategories(): DataFrame = {
    events
      .groupBy("productCategory")
      .agg(count("*").alias("purchases"))
      .sort(desc("purchases"))
      .limit(10)
  }

  def top10MostFrequentlyPurchasedProductsInEachCategory(): DataFrame = {
    val window = Window
      .partitionBy("productCategory")
      .orderBy($"purchases".desc, $"productName")
    events
      .groupBy("productCategory", "productName")
      .agg(count("*").alias("purchases"))
      .withColumn("rank", row_number.over(window))
      .where($"rank" <= 10)
  }

  def top10CountriesWithTheHighestMoneySpending(): DataFrame = {
    val countries = spark.read.format("csv")
      .schema(Encoders.product[Country].schema)
      .option("header", "true")
      .option("inferSchema", "true")
      .load(config.getString("capstone.spark.etl.input.countries"))
      .as[Country]

    val ipBlocks = spark.read.format("csv")
      .schema(Encoders.product[IPBlock].schema)
      .option("header", "true")
      .option("inferSchema", "true")
      .load(config.getString("capstone.spark.etl.input.ipBlocks"))
      .as[IPBlock]

    val isIpOfSubnetUdf = udf(utils.isIpOfSubnet)
    events
      .joinWith(
        ipBlocks.as("ipBlocks")
          .joinWith(countries.as("countries"), $"ipBlocks.geonameId" === $"countries.geonameId"),
        isIpOfSubnetUdf($"clientIpAddress", $"_1.network")
      )
      .groupBy("_2._2.countryName")
      .agg(sum("_1.productPrice").as("price"))
      .orderBy($"price".desc)
      .limit(10)
  }
}

case class Event(
                  productName: String,
                  productPrice: Int,
                  purchaseTime: String,
                  productCategory: String,
                  clientIpAddress: String
                )

case class IPBlock(
                    network: String,
                    geonameId: Long,
                    registeredCountryGeonameId: Long,
                    representedCountryGeonameId: Long,
                    isAnonymousProxy: Int,
                    isSatelliteProvider: Int
                  )

case class Country(
                    geonameId: Long,
                    localeCode: String,
                    continentCode: String,
                    continentName: String,
                    countryIsoCode: String,
                    countryName: String,
                    isInEuropeanUnion: Int
                  )


