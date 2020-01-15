name := "data-generator"

version := "0.1"

scalaVersion := "2.12.10"

assemblyJarName in assembly := "data-generator.jar"

libraryDependencies += "com.opencsv" % "opencsv" % "5.0"
libraryDependencies += "com.typesafe" % "config" % "1.4.0"