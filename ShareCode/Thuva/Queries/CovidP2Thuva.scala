import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{asc, avg, col, desc, isnull, month, not, to_date, udf, when, year}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.commons.io.FileUtils

import scala.util.Try
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.Try

object CovidP2Thuva {
 def UsDeathDataByMonth(): Unit = {
    val MonthDayCount = Array(0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)
    println("Loading Covid Death Data from CSV...........")
    var df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("input/time_series_covid_19_deaths_US.csv")
   df = df.withColumnRenamed("Province_state", "USState")
    var LastDayOfMonth = ""
    var Month = 4
    var year = 20
    var TableColumn = Seq(col("USState"), col("Population"))
    var SumOfMap = Map("Population" -> "sum")
    while (!(Month == 4 && year == 21)) {
      LastDayOfMonth = Month + "/" + MonthDayCount(Month) + "/" + year
      SumOfMap += (LastDayOfMonth -> "sum")

      TableColumn = TableColumn :+ col(LastDayOfMonth)
      Month += 1
      if (Month == 13) {
        Month = 1
        year += 1
      }
    }
    df = df
      .select(TableColumn: _*)
      .groupBy("USState")
      .agg(SumOfMap)
      .withColumnRenamed("sum(Population)", "Population")
    Month = 4
    year = 20
    while (!(Month == 4 && year == 21)) {
      LastDayOfMonth = Month + "/" + MonthDayCount(Month) + "/" + year
      df = df.withColumnRenamed(s"sum(${LastDayOfMonth})", LastDayOfMonth)
      //df = df.withColumnRenamed(s"avg(${LastDayOfMonth})", LastDayOfMonth)
      Month += 1

      if (Month == 13) {
        Month = 1
        year += 1}}
   df = df
      .select(TableColumn: _*)
      .orderBy(desc("Population"))

    df.show(false)
    FileCreate.outputJson("USDeathSumByMonth", df)
  }

  def DeathVSRecoverPercentage(): Unit = {
    val df1 = spark.read.option("header", "true").csv("input/covid_19_data.csv")
    df1.createOrReplaceTempView("CovidWorld")
    println("Top 10 Counries which has best Recovery Percentage against Confirmed Cases..")
    spark.sql("SELECT  `Country/Region` AS Country, SUM(Confirmed) AS TotalConfirmed,SUM(Recovered) AS TotalRecovered ,ROUND((SUM(Recovered) * 100 )/SUM(Confirmed))  as RecoverPercentage FROM CovidWorld GROUP BY  `Country/Region` order by  RecoverPercentage DESC").show(10)

    println("_____________________________________________________________________________")
    println("Top 10 Counries which has worst Death Percentage against Confirmed Cases..")

    spark.sql("SELECT  `Country/Region` AS Country, SUM(Confirmed) AS TotalConfirmed,SUM(Deaths) AS TotalDeaths ,ROUND((SUM(Deaths) * 100 )/SUM(Confirmed))  as DeathPercentage FROM CovidWorld GROUP BY  `Country/Region` order by  DeathPercentage DESC").show(10)

  }

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

  def main(args: Array[String]) {
UsDeathDataByMonth()
    DeathVSRecoverPercentage()
  }

  }


