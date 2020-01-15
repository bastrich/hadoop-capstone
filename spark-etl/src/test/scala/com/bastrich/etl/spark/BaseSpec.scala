package com.bastrich.etl.spark

import com.bastrich.etl.spark.rdd.SparkETL_RDD
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest._

import scala.collection.JavaConverters._

trait BaseSpec extends Matchers {

  val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("spark test")
      .getOrCreate()
  }

  val config = ConfigFactory.parseMap(
    Map[String, Object](
      "capstone.spark.etl.input.events" -> getClass.getResource("/events.csv").toURI.getPath,
      "capstone.spark.etl.input.countries" -> getClass.getResource("/countries.csv").toURI.getPath,
      "capstone.spark.etl.input.ipBlocks" -> getClass.getResource("/ipBlocks.csv").toURI.getPath
    ).asJava
  )

  def assertTop10MostFrequentlyPurchasedCategories(actual: List[(String, Long)]): Unit = {
    val expected = List(
      ("c1", 72L),
      ("c2", 9L),
      ("c3", 8L),
      ("c4", 7L),
      ("c5", 6L),
      ("c6", 5L),
      ("c7", 4L),
      ("c8", 3L),
      ("c9", 2L),
      ("c10", 1L)
    )

    actual should contain theSameElementsAs expected
  }

  def assertTop10MostFrequentlyPurchasedProductsInEachCategory(actual: List[(String, String, Long, Int)]): Unit = {
    val expected = List(
      ("c1", "p1", 25L, 1),
      ("c1", "p2", 9L, 2),
      ("c1", "p3", 8L, 3),
      ("c1", "p4", 7L, 4),
      ("c1", "p5", 6L, 5),
      ("c1", "p6", 5L, 6),
      ("c1", "p7", 4L, 7),
      ("c1", "p8", 3L, 8),
      ("c1", "p9", 2L, 10),
      ("c1", "p10", 2L, 9),
      ("c2", "p1", 9L, 1),
      ("c3", "p1", 8L, 1),
      ("c4", "p1", 6L, 1),
      ("c4", "p5", 1L, 2),
      ("c5", "p1", 6L, 1),
      ("c6", "p1", 5L, 1),
      ("c7", "p1", 4L, 1),
      ("c8", "p1", 3L, 1),
      ("c9", "p1", 2L, 1),
      ("c10", "p1", 1L, 1)
    )

    actual should contain theSameElementsAs expected
  }

  def assertTop10CountriesWithTheHighestMoneySpending(actual: List[(String, Long)]): Unit = {
    val expected = List(
      ("Country1", 630L),
      ("Country2", 150L),
      ("Country3", 100L),
      ("Country4", 70L),
      ("Country6", 60L),
      ("Country5", 50L),
      ("Country7", 40L),
      ("Country8", 30L),
      ("Country9", 20L),
      ("Country12", 10L)
    )

    actual should contain theSameElementsAs expected
  }

}
