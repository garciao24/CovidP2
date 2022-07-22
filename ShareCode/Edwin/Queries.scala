import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}



object Queries{
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
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

      /*Table for Cities*/
      spark.sql("CREATE TABLE IF NOT EXISTS cities AS SELECT SNo, province_state FROM parquet_view")



       /*Table for Countries*/
      spark.sql("CREATE TABLE IF NOT EXISTS countries AS SELECT SNo, region_country FROM parquet_view")



        /*Table for covid_data*/
      spark.sql("CREATE TABLE IF NOT EXISTS covid_data AS SELECT SNo, ObservationDate, Last_Update, " +
        "Confirmed, Deaths, Recovered FROM parquet_view")


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

    spark.sql("SELECT province_state, COUNT(confirmed) AS Total_Cases FROM cities c JOIN covid_data cov ON (c.sno = cov.sno) JOIN countries co " +
      "ON (c.sno = co.sno) WHERE co.region_country = 'US' GROUP BY province_state " +
      "HAVING COUNT(confirmed) > 50 ORDER BY Total_Cases ASC LIMIT 10").show()

    /*    - What are the countries with most covid cases during the pandemic?

          Enter your query here
     */
    spark.sql("SELECT region_country, COUNT(confirmed) AS Cases_Confirmed FROM countries co JOIN covid_data cov ON(co.sno = cov.sno) " +
      "GROUP BY region_country HAVING COUNT(confirmed) > 200 ORDER BY Cases_confirmed DESC").show()

/*    - What are the countries with most recovered covid cases during the pandemic?

      Enter your query here
 */
    spark.sql("SELECT region_country, COUNT(recovered) AS Total_Recovered FROM countries co JOIN covid_data cov ON(co.sno = cov.sno) " +
      "GROUP BY region_country HAVING COUNT(confirmed) > 200 ORDER BY Total_Recovered DESC").show()
  }

}
