
package com
import scala.io.StdIn.readInt

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

object CovidTestExport{

  System.setProperty("hadoop.home.dir", "C:\\Hadoop") //spark session for windows
  val spark = SparkSession
    .builder
    .appName("hello hive")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()
  println("Sucessfully Created Spark Session")
  spark.sparkContext.setLogLevel("ERROR")

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  def main(args: Array[String]): Unit = {

    val df = spark.read.format("csv").option("header", "true").load("hdfs://localhost:9000/user/hive/CSVInput/covid_19_data.csv")
    df.createOrReplaceTempView("CovidCase")
    spark.sql("SELECT * FROM CovidCase where SNo BETWEEN 250 AND 300  ;").show()
//Export as Json
    println("Do you want to export as json file?\n 1 - Yes \n0 - No")
    val x = readInt()
    x match {
      case 0 => "Ignored"
      case 1 => {
        spark.sql( "SELECT * FROM CovidCase where SNo BETWEEN 250 AND 300;").write.format("org.apache.spark.sql.json").mode("overwrite").save(s"hdfs://localhost:9000/user/hive/JSONOutput/Cases")
        println(s"Saved Successfully")
      }
      case _ => println("Invalid input")
    }

   }
}

