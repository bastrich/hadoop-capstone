package com.bastrich.etl.spark.datasets

import com.bastrich.etl.spark.BaseSpec
import org.scalatest.FunSpec

class SparkETL_Datasets_Spec extends FunSpec with BaseSpec {

  it("test top 10 most frequently purchased categories") {
    val sparkRunner = new SparkETL_Datasets(spark, config)
    assertTop10MostFrequentlyPurchasedCategories(
      sparkRunner.top10MostFrequqentlyPurchasedCategories().collect().map( row => (row.getString(0), row.getLong(1))).toList
    )
  }

  it("test top 10 most frequently purchased products in each category") {
    val sparkRunner = new SparkETL_Datasets(spark, config)
    assertTop10MostFrequentlyPurchasedProductsInEachCategory(
      sparkRunner.top10MostFrequentlyPurchasedProductsInEachCategory().collect().map(row =>
        (row.getString(0), row.getString(1), row.getLong(2), row.getInt(3))
      ).toList
    )
  }

  it("test top 10 countries with the highest money spending") {
    val sparkRunner = new SparkETL_Datasets(spark, config)
    assertTop10CountriesWithTheHighestMoneySpending(
      sparkRunner.top10CountriesWithTheHighestMoneySpending().collect().map(row =>
        (row.getString(0), row.getLong(1))
      ).toList
    )
  }
}
