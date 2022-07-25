package P2

import P2.Main.session
import P2.P2tempviews.{df4, df6}
import org.apache.spark.sql.functions.{col, desc}

object CovidP2Thuva {
  // 1. Total Death count of USA states by Month wise - UsDeathDataByMonth()
  // 2. Find best 10 recovery rates countries and worst 10 Death rates countries  - DeathVSRecoverPercentage()
  // 3. Which top 20 dates Recorded highest Confirmed ccases -  TopDays()
  
  def UsDeathDataByMonth(v1:Boolean,v2:Boolean): Unit = {
    val MonthDayCount = Array(0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)
    session.logger.info("Loading Sum of Covid Death Data By Month wise ...........")
    //var df0 = session.spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://localhost:9000/user/hive/warehouse/time_series_covid_19_deaths_US.csv")

    var df0 = df4.withColumnRenamed("Province_state", "USState")
    var LastDayOfMonth = ""
    var Month = 1
    var year = 20
    var TableColumn = Seq(col("USState"), col("Population"))
    var SumOfMap = Map("Population" -> "sum")
    while (!(Month == 5 && year == 21)) {
      LastDayOfMonth = Month + "/" + MonthDayCount(Month) + "/" + year
      SumOfMap += (LastDayOfMonth -> "sum")

      TableColumn = TableColumn :+ col(LastDayOfMonth)
      Month += 1
      if (Month == 13) {
        Month = 1
        year += 1
      }
    }
    df0 = df0
      .select(TableColumn: _*)
      .groupBy("USState")
      .agg(SumOfMap)
      .withColumnRenamed("sum(Population)", "Population")
    Month = 1
    year = 20
    while (!(Month == 5 && year == 21)) {
      LastDayOfMonth = Month + "/" + MonthDayCount(Month) + "/" + year
      df0 = df0.withColumnRenamed(s"sum(${LastDayOfMonth})", LastDayOfMonth)
      //df = df.withColumnRenamed(s"avg(${LastDayOfMonth})", LastDayOfMonth)
      Month += 1

      if (Month == 13) {
        Month = 1
        year += 1
      }
    }
    df0 = df0
      .select(TableColumn: _*)
      .orderBy(desc("Population"))

    if(v1){
      df0.show(false)
    }
    if(v2){
      file.outputJson("USDeathSumByMonth", df0)
      file.outputcsv("USDeathSumByMonth", df0)
    }
  }

  def DeathVSRecoverPercentage(v1:Boolean,v2:Boolean): Unit = {
    //val df6 = session.spark.read.option("header", "true").csv("hdfs://localhost:9000/user/hive/warehouse/covid_19_data.csv")//////
    df6.createOrReplaceTempView("CovidWorld")
    //println("Top 10 Countries which has best Recovery Percentage against Confirmed Cases..")
    val fin = session.spark.sql("SELECT  `Country/Region` AS Country, SUM(Confirmed) AS TotalConfirmed,SUM(Recovered) AS TotalRecovered ,ROUND((SUM(Recovered) * 100 )/SUM(Confirmed))  as RecoverPercentage FROM CovidWorld GROUP BY  `Country/Region` order by  RecoverPercentage DESC").toDF()

    //println("_____________________________________________________________________________")
    //println("Top 10 Counries which has worst Death Percentage against Confirmed Cases..")

    val fin2 = session.spark.sql("SELECT  `Country/Region` AS Country, SUM(Confirmed) AS TotalConfirmed,SUM(Deaths) AS TotalDeaths ,ROUND((SUM(Deaths) * 100 )/SUM(Confirmed))  as DeathPercentage FROM CovidWorld GROUP BY  `Country/Region` order by  DeathPercentage DESC").toDF()

    if(v1){
      println("Top 10 Countries which has best Recovery Percentage against Confirmed Cases..")
      fin.show(10)
      println("_____________________________________________________________________________")
      println("Top 10 Countries which has worst Death Percentage against Confirmed Cases..")
      fin2.show(10)
    }
    if(v2){
      file.outputJson("RecoverPercentage",fin)
      file.outputcsv("RecoverPercentage",fin)
      file.outputJson("DeathPercentage",fin2)
      file.outputcsv("DeathPercentage",fin2)
    }
  }



  def TopDays(v1:Boolean,v2:Boolean): Unit = {
    session.logger.info("Load top 20 Days Recorded highest Confirmed Cases between 2020 -2021")

    val df1 = session.spark.read.option("header", "true").csv("hdfs://localhost:9000/user/hive/warehouse/covid_19_data.csv")
    df1.createOrReplaceTempView("CovidCases")

    session.spark.sql(" SELECT ObservationDate , `Province/State`, `Country/Region`, max(Confirmed) AS MaxCases " +
      " FROM CovidCases " +
      " GROUP BY ObservationDate,`Country/Region`,`Province/State` ").createTempView("Query1")

    session.spark.sql(" SELECT ObservationDate, FLOOR(sum(MaxCases)) AS CombineDailyCases" +
      " FROM Query1" +
      " GROUP BY ObservationDate" +
      " ORDER BY CombineDailyCases ASC").createTempView("Query2") // Sum Daily Cases by Date wise

    session.spark.sql(" SELECT ObservationDate, CombineDailyCases, " +
      " LAG(CombineDailyCases,1) OVER(" +
      " ORDER BY CombineDailyCases ASC) AS PreviousDailyCases" +
      " FROM Query2" +
      " ORDER BY CombineDailyCases ASC").createTempView("Query3") //Check Previus Values

    val fin = session.spark.sql(" SELECT ObservationDate as Date, CombineDailyCases - IFNULL(PreviousDailyCases,0) AS NewCases" +
      " FROM Query3" +
      " ORDER BY NewCases DESC").toDF()

    if(v1){
      fin.show()
    }
    if(v2){
      file.outputJson("TopDays",fin)
      file.outputcsv("TopDays",fin)
    }
  }


  def run():Unit = {
//    UsDeathDataByMonth()
//    DeathVSRecoverPercentage()
//    TopDays()

  }

}
