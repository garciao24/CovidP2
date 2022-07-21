import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, Dataset}

import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType}
import org.apache.spark.sql.Row
import java.util.Scanner


object Queries{
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Hadoop3")
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

    def createTables(): Unit = {


      val dfcovid = spark.read.option("header", true).csv("hdfs://localhost:9000/user/hive/warehouse/covid_19_data.csv")
      dfcovid.write.mode(SaveMode.Overwrite).csv("hdfs://localhost:9000/user/hive/warehouse/new_covid_19_data")

      val sc=spark.sparkContext
      val rddCovidFromFile = sc.textFile("/user/hive/warehouse/new_covid_19_data")
      val rddCovid = rddCovidFromFile.map(f=>{f.split(",")})
      rddCovid.toDS.show(false)

      import spark.implicits._
      case class City(CityID: Long, Province_State: String)
      val dfcit=rddCovid.map(attributes => City(attributes(0).trim.toLong, attributes(2))).toDF()
      dfcit.createOrReplaceTempView("cities")
      spark.sql("SELECT * FROM cities").show()

      case class Country(CountryID: Long, Country: String)
      val dfcount=rddCovid.map(attributes => Country(attributes(0).trim.toLong, attributes(3))).toDF()
      dfcount.createOrReplaceTempView("countries")
      spark.sql("SELECT * FROM countries").show()

      case class Covid_data(SNo: Long, ObservationDate: String, Last_Update: String, Confirmed: String, Deaths: String, Recovered: String)
      val dfcovid=rddCovid.map(attributes => Covid_data(attributes(0).trim.toLong, attributes(1), attributes(4), attributes(5), attributes(6), attributes(7))).toDF()
      dfcovid.createOrReplaceTempView("covid")
      spark.sql("SELECT * FROM covid").show()

    }

/*    - How many cases were confirmed worldwide during the second quarter of 2020?

      Enter your query here
 */

/*    - How many people were recovered worldwide by the last quarter of 2020

      Enter your query here
 */


/*    - What are the top 10 cities with number of deaths in the US?

      Enter your query here
 */


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
