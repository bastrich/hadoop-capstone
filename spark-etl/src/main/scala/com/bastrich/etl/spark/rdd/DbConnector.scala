package com.bastrich.etl.spark.rdd

import java.sql.{Connection, DriverManager}

import com.typesafe.config.Config
class DbConnector(val config: Config) {

  implicit class DbWriteable[T](val value: T) {
    def withDbConnection(func: (T, Connection) => Unit): Unit = {
      val dbConnection = DriverManager.getConnection(
        config.getString("capstone.spark.etl.output.jdbc.url"),
        config.getString("capstone.spark.etl.output.jdbc.user"),
        config.getString("capstone.spark.etl.output.jdbc.password")
      )
      try {
        func(value, dbConnection)
      } catch {
        case e: Exception => throw e
      } finally {
        dbConnection.close()
      }
    }
  }

}

