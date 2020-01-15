package com.bastrich.etl.spark.rdd

import com.bastrich.etl.spark.BaseSpec
import org.scalatest.FunSpec

class SparkETL_RDD_Spec extends FunSpec with BaseSpec {

  it("test top 10 most frequently purchased categories") {
    val sparkRunner = new SparkETL_RDD(spark, config)
    assertTop10MostFrequentlyPurchasedCategories(sparkRunner.top10MostFrequentlyPurchasedCategories())
  }

  it("test top 10 most frequently purchased products in each category") {
    val sparkRunner = new SparkETL_RDD(spark, config)
    assertTop10MostFrequentlyPurchasedProductsInEachCategory(
      sparkRunner.top10MostFrequentlyPurchasedProductsInEachCategory()
        .flatMap( category => {
          category._2.map(product => (category._1, product._1._1, product._1._2, product._2))
        })
    )
  }

  it("test top 10 countries with the highest money spending") {
    val sparkRunner = new SparkETL_RDD(spark, config)
    assertTop10CountriesWithTheHighestMoneySpending(sparkRunner.top10CountriesWithTheHighestMoneySpending())
  }
}
