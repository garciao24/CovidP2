import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, Dataset}

import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType}
import org.apache.spark.sql.Row
import java.util.Scanner


object Queries{
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val scanner = new Scanner(System.in)
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    spark.sparkContext.setLogLevel("ERROR")

    println("created spark session")

    createTables()

    def createTables(): Unit = {

      import spark.implicits._

//      val dfcovid = spark.read.option("header", true).csv("hdfs://localhost:9000/user/hive/warehouse/covid_19_data.csv")
//      dfcovid.write.mode(SaveMode.Overwrite).csv("hdfs://localhost:9000/user/hive/warehouse/new_covid_19_data")
      val dfcovidFromFile = spark.read.option("delimiter", ",").option("inferSchema","true").option("header", "true")
                .csv(s"hdfs://localhost:9000/user/hive/warehouse/covid_19_data.csv")

      dfcovidFromFile.createOrReplaceTempView("cd")

      val dfCovid_data=(dfcovidFromFile.withColumnRenamed("Country/Region", "region_country")
        .withColumnRenamed("Province/State", "province_state")
        .withColumnRenamed("Last Update", "Last_Update")
        .withColumn("Confirmed", col("Confirmed").cast("int"))
        .withColumn("Deaths", col("Deaths").cast("int"))
        .withColumn("Recovered", col("Recovered").cast("int"))
        )

      dfCovid_data.show()
      dfCovid_data.createOrReplaceTempView("covid_view")
      dfCovid_data.repartition(10).write.mode("overwrite").save("hdfs://localhost:9000/user/hive/warehouse/new_covid_19_data.parquet")
      val parquetfiledf = spark.read.parquet("hdfs://localhost:9000/user/hive/warehouse/new_covid_19_data.parquet")
      parquetfiledf.createOrReplaceTempView("parquet_view")
      spark.sql("SELECT * FROM parquet_view order by SNo ASC").show(25)

      /*Table for Cities*/
      spark.sql("CREATE TABLE IF NOT EXISTS cities AS SELECT SNo, province_state FROM parquet_view")
      spark.sql("select * from cities order by SNo ASC").show()


       /*Table for Countries*/
      spark.sql("CREATE TABLE IF NOT EXISTS countries AS SELECT SNo, region_country FROM parquet_view")
      spark.sql("select * from countries order by SNo ASC").show()


        /*Table for covid_data*/
      spark.sql("CREATE TABLE IF NOT EXISTS covid_data AS SELECT SNo, ObservationDate, Last_Update, " +
        "Confirmed, Deaths, Recovered FROM parquet_view")
      spark.sql("select * from covid_data order by SNo ASC").show()

    }

/*    - How many cases were confirmed worldwide during the second quarter of 2020?

      Enter your query here
 */
    spark.sql("SELECT ObservationDate, COUNT(confirmed) AS Total_confirmed FROM covid_data where ObservationDate BETWEEN '04/01/2020' AND '06/30/2020' " +
      "GROUP BY ObservationDate ORDER BY ObservationDate ASC").show()

/*    - How many people were recovered worldwide by the last quarter of 2020

      Enter your query here
 */
    spark.sql("SELECT ObservationDate, COUNT(recovered) AS Total_Recovered FROM covid_data where ObservationDate BETWEEN '10/01/2020' AND '12/31/2020' " +
      "GROUP BY ObservationDate ORDER BY ObservationDate ASC").show()


/*    - What are the top 10 cities with number of deaths in the US?

      Enter your query here
 */
    spark.sql("SELECT province_state, COUNT(deaths) AS Total_Deaths FROM cities c JOIN covid_data cov ON (c.sno = cov.sno) JOIN countries co " +
      "ON (c.sno = co.sno) WHERE co.region_country = 'US' GROUP BY province_state ORDER BY Total_Deaths DESC LIMIT 10").show()

/*    - What are the top 10 cities with least COVID cases in the US?

      Enter your query here
 */


/*    - What were the continent with most covid cases during 2020?

      Enter your query here
 */


/*    - What were the continent with most recovered covid cases during 2020?

      Enter your query here
 */

  }

}
