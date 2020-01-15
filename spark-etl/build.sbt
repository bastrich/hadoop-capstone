name := "spark-etl"

version := "0.1"

scalaVersion := "2.12.10"

assemblyJarName in assembly := "spark-etl.jar"

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.0-bastrich1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.0-bastrich1" % "provided"
libraryDependencies += "com.typesafe" % "config" % "1.4.0"
libraryDependencies += "org.postgresql" % "postgresql" % "42.2.8"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % "test"
libraryDependencies += "MrPowers" % "spark-fast-tests" % "0.20.0-s_2.11"

