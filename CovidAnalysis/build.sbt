
ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "Project1"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.1"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.2.1"
libraryDependencies ++= Seq("org.apache.hadoop" % "hadoop-client" % "3.3.2")
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.29"

libraryDependencies += "com.lihaoyi" %% "ujson" % "2.0.0"
libraryDependencies += "com.lihaoyi" %% "requests" % "0.7.1"
libraryDependencies += "com.lihaoyi" %% "os-lib" % "0.8.1"

libraryDependencies +="com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.13.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.1" % "provided",
  "org.apache.commons" % "commons-lang3" % "3.12.0")

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5"
libraryDependencies += "com.typesafe" % "config" % "1.4.2"

libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.17.2"
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.17.2"
libraryDependencies += "org.apache.logging.log4j" %% "log4j-api-scala" % "12.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.3.2"
libraryDependencies += "org.apache.commons" % "commons-io" % "1.3.2"
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "3.3.2"