package com.bastrich.etl.spark.rdd

import com.bastrich.etl.spark.utils
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

class SparkETL_RDD( spark: SparkSession,
                    config: Config
                  ) {

  private val events = spark.sparkContext.textFile(config.getString("capstone.spark.etl.input.events"))
    .map(line => {
      val arr = line.split(',')
      (arr(3), arr(0), arr(4), arr(1))
    })

  {
    Class.forName("org.postgresql.Driver")
  }


  def top10MostFrequentlyPurchasedCategories(): List[(String, Long)] = {
    events
      .map(pair => {
        (pair._1, 1L)
      })
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .take(10)
      .toList
  }

  def top10MostFrequentlyPurchasedProductsInEachCategory(): List[(String, List[((String, Long), Int)])] = {
    events
      .map(pair => {
        ((pair._1, pair._2), 1L)
      })
      .reduceByKey(_+_)
      .map(pair => {
        (pair._1._1, (pair._1._2, pair._2))
      })
      .groupByKey()
      .mapValues(iter => {
        iter.toList.sortBy(v => (-v._2, v._1)).zip(Stream from 1).take(10)
      })
      .collect()
      .toList
  }

  def top10CountriesWithTheHighestMoneySpending(): List[(String, Long)] = {


    val ipBlocks = spark.sparkContext.textFile(config.getString("capstone.spark.etl.input.ipBlocks"))
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map(line => {
        val arr = line.split(',')
        (arr(1), arr(0))
      })

    val countries = spark.sparkContext.textFile(config.getString("capstone.spark.etl.input.countries"))
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map(line => {
        val arr = line.split(',')
        (arr(0), arr(5).replaceAll("^\"|\"$", ""))
      })

    events
      .cartesian(
        ipBlocks
          .join(countries)
          .map(_._2)
      )
      .filter(event =>
        utils.isIpOfSubnet(event._1._3, event._2._1)
      )
      .map(event =>
        (event._2._2, event._1._4.toLong)
      )
      .aggregateByKey(0L)(_ + _, _ + _)
      .sortBy(_._2, ascending = false)
      .take(10)
      .toList
  }
}
