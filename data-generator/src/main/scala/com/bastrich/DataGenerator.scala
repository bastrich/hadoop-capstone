package com.bastrich

import java.io.OutputStreamWriter
import java.net.{InetAddress, Socket}
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit.DAYS
import com.typesafe.config.ConfigFactory

import com.opencsv.{CSVWriter, ICSVWriter}

import scala.collection.mutable
import scala.util.Random

case class Product(name: String, categories: Array[String], prices: Array[Int])

object DataGenerator {
  private val config = ConfigFactory.load().resolve()

  private val DESTINATION_HOST = config.getString("generator.destination.host")
  private val DESTINATION_PORT = config.getInt("generator.destination.port")
  private val BATCH_SIZE = config.getInt("generator.batch.size")
  private val BATCH_INTERVAL_MS = config.getInt("generator.batch.interval.ms")

  private val AMOUNT_OF_CATEGORIES = config.getInt("generator.target.categories.number")
  private val AMOUNT_OF_PRODUCTS = config.getInt("generator.target.products.number")
  private val AMOUNT_OF_EVENTS_TO_GENERATE = config.getInt("generator.target.events.number")
  private val PURCHASE_FROM_DATE =  LocalDateTime.parse(config.getString("generator.target.date.from") + "T00:00:00")
  private val PURCHASE_TO_DATE = LocalDateTime.parse(config.getString("generator.target.date.to") + "T00:00:00")
  private val NUMBER_OF_STANDARD_DEVIATIONS_TO_CONSIDER = config.getInt("generator.target.deviations.number")
  private val MAX_PRODUCT_PRICE = config.getInt("generator.target.products.maxPrice")

  private val PRODUCTS = generateProducts()

  def main(args: Array[String]): Unit = {
    val socket = new Socket(InetAddress.getByName(DESTINATION_HOST), DESTINATION_PORT)
    val csvWriter: CSVWriter = new CSVWriter(
      new OutputStreamWriter(socket.getOutputStream),
      ICSVWriter.DEFAULT_SEPARATOR,
      ICSVWriter.NO_QUOTE_CHARACTER,
      ICSVWriter.DEFAULT_ESCAPE_CHARACTER,
      ICSVWriter.DEFAULT_LINE_END
    )

    Range(0, AMOUNT_OF_EVENTS_TO_GENERATE).foreach(index => {
      val product = PRODUCTS(Random.nextInt(PRODUCTS.size))

      csvWriter.writeNext(Array[String](
        product.name,
        product.prices(Random.nextInt(product.prices.length)).toString,
        generatePurchaseDate,
        product.categories(Random.nextInt(product.categories.length)),
        generateClientIpAddress
      ))

      if (index != 0 && index % BATCH_SIZE == 0) {
        csvWriter.flush()
        println(s"Batch ${index/BATCH_SIZE}/${AMOUNT_OF_EVENTS_TO_GENERATE/BATCH_SIZE} sent")
        Thread.sleep(BATCH_INTERVAL_MS)
      }
    })

    csvWriter.close()
    println("Successfully finished generating data!")
  }

  private def generateProducts(): List[Product] = {
    val categories = Range(0, AMOUNT_OF_CATEGORIES).map(_ => {
      Random.alphanumeric.dropWhile(_.isDigit).take(Random.nextInt(15) + 5).mkString
    })

    val result = mutable.HashMap[String, Product]()
    Range(0, AMOUNT_OF_PRODUCTS).foreach(_ => {
      var name = generateProductName
      while (result.contains(name)) {
        name = generateProductName
      }

      val possibleCategories = Random.shuffle(categories).take(Random.nextInt(3) + 1).toArray
      val possiblePrices = Range(0, Random.nextInt(3) + 1).map(_ => { generateProductPrice }).toArray

      result += name -> Product(name, possibleCategories, possiblePrices)
    })
    result.values.toList
  }

  private def generateProductName(): String = Random.alphanumeric.dropWhile(_.isDigit).take(Random.nextInt(20) + 5).mkString

  private def generateProductPrice(): Int = (((getLimitedGaussian + NUMBER_OF_STANDARD_DEVIATIONS_TO_CONSIDER) / NUMBER_OF_STANDARD_DEVIATIONS_TO_CONSIDER * 2) * MAX_PRODUCT_PRICE).toInt

  private def generatePurchaseDate(): String = {
    val date = PURCHASE_FROM_DATE.plusDays(Random.nextInt(DAYS.between(PURCHASE_FROM_DATE, PURCHASE_TO_DATE).toInt - 1))
    val timeNano = (((getLimitedGaussian + NUMBER_OF_STANDARD_DEVIATIONS_TO_CONSIDER) / NUMBER_OF_STANDARD_DEVIATIONS_TO_CONSIDER * 2) * 1000 * 60 * 60 * 24 * 1000 * 1000).toLong

    date.plusNanos(timeNano).format(DateTimeFormatter.ofPattern("yyyy-LL-dd HH:mm:ss"))
  }

  private def generateClientIpAddress(): String = Range(1, 5).map(_ => { Random.nextInt(256) }).mkString(".")

  private def getLimitedGaussian(): Double = {
    var gaussian = Random.nextGaussian()
    while (gaussian > NUMBER_OF_STANDARD_DEVIATIONS_TO_CONSIDER || gaussian < -NUMBER_OF_STANDARD_DEVIATIONS_TO_CONSIDER) {
      gaussian = Random.nextGaussian()
    }
    gaussian
  }
}
