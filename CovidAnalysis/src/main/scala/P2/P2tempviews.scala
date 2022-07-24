package P2

import P2.Main.session
import org.apache.spark.sql.DataFrame

object P2tempviews {
  var df1: DataFrame = _
  var df2: DataFrame = _
  var df3: DataFrame = _
  var df4: DataFrame = _
  var df5: DataFrame = _
  var df6: DataFrame = _



  def CreateTemp():Unit= {
    /*Anything above this has been mostly unchanged (generalized)*/


    // ------------------------------------------------------------- Create Temp Views From HDFS -------------------------------------------------------------
    df1 = session.spark.read.format("csv").option("header", "true").load("hdfs://localhost:9000/user/hive/warehouse/time_series_covid_19_confirmed.csv") // File location in hdfs
    df1.createOrReplaceTempView("CovConImp")

    //session.spark.sql("SELECT * FROM CovConImp;").show()

    df2 = session.spark.read.format("csv").option("header", "true").load("hdfs://localhost:9000/user/hive/warehouse/time_series_covid_19_confirmed_US.csv")
    df2.createOrReplaceTempView("CovConUSImp")

    //session.spark.sql("SELECT * FROM CovConUSImp;").show()

    df3 = session.spark.read.format("csv").option("header", "true").load("hdfs://localhost:9000/user/hive/warehouse/time_series_covid_19_deaths.csv")
    df3.createOrReplaceTempView("CovDeathsImp")

    //session.spark.sql("SELECT * FROM CovDeathsImp;").show()

    df4 = session.spark.read.format("csv").option("header", "true").load("hdfs://localhost:9000/user/hive/warehouse/time_series_covid_19_deaths_US.csv")
    df4.createOrReplaceTempView("CovDeathsUSImp")

    //session.spark.sql("SELECT * FROM CovDeathsUSImp;").show()

    df5 = session.spark.read.format("csv").option("header", "true").load("hdfs://localhost:9000/user/hive/warehouse/time_series_covid_19_recovered.csv")
    df5.createOrReplaceTempView("CovRecImp")

    //session.spark.sql("SELECT * FROM CovRecImp;").show()

    df6 = session.spark.read.format("csv").option("header", "true").load("hdfs://localhost:9000/user/hive/warehouse/covid_19_data.csv")
    df6.createOrReplaceTempView("CovDataImp")

    //session.spark.sql("SELECT * FROM CovDeathsUSImp;").show()

    // ------------------------------------------------------------- Move Temp Views Into Spark Warehouse -------------------------------------------------------------

    df1.write.mode("overwrite").saveAsTable("CovCon")
    //session.spark.sql("SELECT * FROM CovCon").show()

    df2.write.mode("overwrite").saveAsTable("CovConUS")
    //session.spark.sql("SELECT * FROM CovConUS").show()

    df3.write.mode("overwrite").saveAsTable("CovDeaths")
    //session.spark.sql("SELECT * FROM CovDeaths").show()

    df4.write.mode("overwrite").saveAsTable("CovDeathsUS")
    //session.spark.sql("SELECT * FROM CovDeathsUS").show()

    df5.write.mode("overwrite").saveAsTable("CovRec")
    //session.spark.sql("SELECT * FROM CovDeaths").show()

    val df6m = df6.withColumnRenamed("Last Update", "Last_Update")
    df6m.write.mode("overwrite").saveAsTable("CovData")
    //session.spark.sql("SELECT * FROM CovData").show()

    //session.spark.sql("SHOW DATABASES").show()
    //session.spark.sql("SHOW TABLES").show()


    BasicCleaning.runOscar()
    Queries.createTablesE()
  }

}