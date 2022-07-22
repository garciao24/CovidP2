import P2functions.spark
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object P2tempviews {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("Created spark session.")
    /*Anything above this has been mostly unchanged (generalized)*/

    // ------------------------------------------------------------- Create Temp Views From HDFS -------------------------------------------------------------

    val df1 = spark.read.format("csv").option("header","true").load("hdfs://localhost:9000/user/hive/warehouse/Project2/time_series_covid_19_confirmed.csv") // File location in hdfs
    df1.createOrReplaceTempView("CovConImp")

    spark.sql("SELECT * FROM CovConImp;").show()

    val df2 = spark.read.format("csv").option("header","true").load("hdfs://localhost:9000/user/hive/warehouse/Project2/time_series_covid_19_confirmed_US.csv")
    df2.createOrReplaceTempView("CovConUSImp")

    spark.sql("SELECT * FROM CovConUSImp;").show()

    val df3 = spark.read.format("csv").option("header","true").load("hdfs://localhost:9000/user/hive/warehouse/Project2/time_series_covid_19_deaths.csv")
    df3.createOrReplaceTempView("CovDeathsImp")

    spark.sql("SELECT * FROM CovDeathsImp;").show()

    val df4 = spark.read.format("csv").option("header","true").load("hdfs://localhost:9000/user/hive/warehouse/Project2/time_series_covid_19_deaths_US.csv")
    df4.createOrReplaceTempView("CovDeathsUSImp")

    spark.sql("SELECT * FROM CovDeathsUSImp;").show()

    val df5 = spark.read.format("csv").option("header","true").load("hdfs://localhost:9000/user/hive/warehouse/Project2/time_series_covid_19_recovered.csv")
    df3.createOrReplaceTempView("CovRecImp")

    spark.sql("SELECT * FROM CovRecImp;").show()

    val df6 = spark.read.format("csv").option("header","true").load("hdfs://localhost:9000/user/hive/warehouse/Project2/covid_19_data.csv")
    df4.createOrReplaceTempView("CovRecImp")

    spark.sql("SELECT * FROM CovDeathsUSImp;").show()

    // ------------------------------------------------------------- Move Temp Views Into Spark Warehouse -------------------------------------------------------------

    df1.write.mode("overwrite").saveAsTable("CovCon")
    spark.sql("SELECT * FROM CovCon").show()

    df2.write.mode("overwrite").saveAsTable("CovConUS")
    spark.sql("SELECT * FROM CovConUS").show()

    df3.write.mode("overwrite").saveAsTable("CovDeaths")
    spark.sql("SELECT * FROM CovDeaths").show()

    df4.write.mode("overwrite").saveAsTable("CovDeathsUS")
    spark.sql("SELECT * FROM CovDeathsUS").show()

    df5.write.mode("overwrite").saveAsTable("CovRec")
    spark.sql("SELECT * FROM CovDeaths").show()

    val df6m = df6.withColumnRenamed("Last Update","Last_Update")
    df6m.write.mode("overwrite").saveAsTable("CovData")
    spark.sql("SELECT * FROM CovData").show()

    spark.sql("SHOW DATABASES").show()
    spark.sql("SHOW TABLES").show()

    val ColumnNames=df2.columns  // Makes a list of headers/column names.
    println(ColumnNames.mkString(","))

  }
}