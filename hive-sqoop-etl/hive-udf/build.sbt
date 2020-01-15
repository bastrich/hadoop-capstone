name := "hive-udf"

version := "0.1"

scalaVersion := "2.12.10"

assemblyJarName in assembly := "hive-udf.jar"

resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"

libraryDependencies += "org.apache.hive" %  "hive-exec" % "3.1.3-bastrich1" % "provided"