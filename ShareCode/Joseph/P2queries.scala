import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import scala.io.StdIn.readLine

object P2queries{
  def main(args: Array[String]): Unit = {

  }
  System.setProperty("hadoop.home.dir", "C:\\Hadoop3")
  val spark = SparkSession
    .builder
    .appName("hello hive")
    .config("spark.master", "local[*]")
    .enableHiveSupport()
    .getOrCreate()
  Logger.getLogger("org").setLevel(Level.ERROR)
  print("spark session created")
  spark.sparkContext.setLogLevel("ERROR")

  spark.sql("DROP table IF EXISTS CovConUS")
  spark.sql("create table IF NOT EXISTS CovConUS(UID Int, iso2 String, iso3 String, code3 Int, FIPS Int, " +
    "Admin2 String, Province_State String, Country_Region String, Lat Float, Long_ Float, Combined_Key String, " +
    "`1/22/2020` Int, `1/23/2020` Int, `1/24/2020` Int, `1/25/2020` Int, `1/26/2020` Int, `1/27/2020` Int, `1/28/2020` Int, " +
    "`1/29/2020` Int, `1/30/2020` Int, `1/31/2020` Int, `2/1/2020` Int, `2/2/2020` Int, `2/3/2020` Int, `2/4/2020` Int, " +
    "`2/5/2020` Int, `2/6/2020` Int, `2/7/2020` Int, `2/8/2020` Int, `2/9/2020` Int, `2/10/2020` Int, `2/11/2020` Int, " +
    "`2/12/2020` Int, `2/13/2020` Int, `2/14/2020` Int, `2/15/2020` Int, `2/16/2020` Int, `2/17/2020` Int, `2/18/2020` Int, " +
    "`2/19/2020` Int, `2/20/2020` Int, `2/21/2020` Int, `2/22/2020` Int, `2/23/2020` Int, `2/24/2020` Int, `2/25/2020` Int, " +
    "`2/26/2020` Int, `2/27/2020` Int, `2/28/2020` Int, `2/29/2020` Int, `3/1/2020` Int, `3/2/2020` Int, `3/3/2020` Int, " +
    "`3/4/2020` Int, `3/5/2020` Int, `3/6/2020` Int, `3/7/2020` Int, `3/8/2020` Int, `3/9/2020` Int, `3/10/2020` Int, " +
    "`3/11/2020` Int, `3/12/2020` Int, `3/13/2020` Int, `3/14/2020` Int, `3/15/2020` Int, `3/16/2020` Int, `3/17/2020` Int, " +
    "`3/18/2020` Int, `3/19/2020` Int, `3/20/2020` Int, `3/21/2020` Int, `3/22/2020` Int, `3/23/2020` Int, `3/24/2020` Int, " +
    "`3/25/2020` Int, `3/26/2020` Int, `3/27/2020` Int, `3/28/2020` Int, `3/29/2020` Int, `3/30/2020` Int, `3/31/2020` Int, " +
    "`4/1/2020` Int, `4/2/2020` Int, `4/3/2020` Int, `4/4/2020` Int, `4/5/2020` Int, `4/6/2020` Int, `4/7/2020` Int, " +
    "`4/8/2020` Int, `4/9/2020` Int, `4/10/2020` Int, `4/11/2020` Int, `4/12/2020` Int, `4/13/2020` Int, `4/14/2020` Int, " +
    "`4/15/2020` Int, `4/16/2020` Int, `4/17/2020` Int, `4/18/2020` Int, `4/19/2020` Int, `4/20/2020` Int, `4/21/2020` Int, " +
    "`4/22/2020` Int, `4/23/2020` Int, `4/24/2020` Int, `4/25/2020` Int, `4/26/2020` Int, `4/27/2020` Int, `4/28/2020` Int, " +
    "`4/29/2020` Int, `4/30/2020` Int, `5/1/2020` Int, `5/2/2020` Int, `5/3/2020` Int, `5/4/2020` Int, `5/5/2020` Int, " +
    "`5/6/2020` Int, `5/7/2020` Int, `5/8/2020` Int, `5/9/2020` Int, `5/10/2020` Int, `5/11/2020` Int, `5/12/2020` Int, " +
    "`5/13/2020` Int, `5/14/2020` Int, `5/15/2020` Int, `5/16/2020` Int, `5/17/2020` Int, `5/18/2020` Int, `5/19/2020` Int, " +
    "`5/20/2020` Int, `5/21/2020` Int, `5/22/2020` Int, `5/23/2020` Int, `5/24/2020` Int, `5/25/2020` Int, `5/26/2020` Int, " +
    "`5/27/2020` Int, `5/28/2020` Int, `5/29/2020` Int, `5/30/2020` Int, `5/31/2020` Int, `6/1/2020` Int, `6/2/2020` Int, " +
    "`6/3/2020` Int, `6/4/2020` Int, `6/5/2020` Int, `6/6/2020` Int, `6/7/2020` Int, `6/8/2020` Int, `6/9/2020` Int, `6/10/2020` Int, " +
    "`6/11/2020` Int, `6/12/2020` Int, `6/13/2020` Int, `6/14/2020` Int, `6/15/2020` Int, `6/16/2020` Int, `6/17/2020` Int, " +
    "`6/18/2020` Int, `6/19/2020` Int, `6/20/2020` Int, `6/21/2020` Int, `6/22/2020` Int, `6/23/2020` Int, `6/24/2020` Int, " +
    "`6/25/2020` Int, `6/26/2020` Int, `6/27/2020` Int, `6/28/2020` Int, `6/29/2020` Int, `6/30/2020` Int, `7/1/2020` Int, " +
    "`7/2/2020` Int, `7/3/2020` Int, `7/4/2020` Int, `7/5/2020` Int, `7/6/2020` Int, `7/7/2020` Int, `7/8/2020` Int, " +
    "`7/9/2020` Int, `7/10/2020` Int, `7/11/2020` Int, `7/12/2020` Int, `7/13/2020` Int, `7/14/2020` Int, `7/15/2020` Int, " +
    "`7/16/2020` Int, `7/17/2020` Int, `7/18/2020` Int, `7/19/2020` Int, `7/20/2020` Int, `7/21/2020` Int, `7/22/2020` Int, " +
    "`7/23/2020` Int, `7/24/2020` Int, `7/25/2020` Int, `7/26/2020` Int, `7/27/2020` Int, `7/28/2020` Int, `7/29/2020` Int, " +
    "`7/30/2020` Int, `7/31/2020` Int, `8/1/2020` Int, `8/2/2020` Int, `8/3/2020` Int, `8/4/2020` Int, `8/5/2020` Int, " +
    "`8/6/2020` Int, `8/7/2020` Int, `8/8/2020` Int, `8/9/2020` Int, `8/10/2020` Int, `8/11/2020` Int, `8/12/2020` Int, " +
    "`8/13/2020` Int, `8/14/2020` Int, `8/15/2020` Int, `8/16/2020` Int, `8/17/2020` Int, `8/18/2020` Int, `8/19/2020` Int, " +
    "`8/20/2020` Int, `8/21/2020` Int, `8/22/2020` Int, `8/23/2020` Int, `8/24/2020` Int, `8/25/2020` Int, `8/26/2020` Int, " +
    "`8/27/2020` Int, `8/28/2020` Int, `8/29/2020` Int, `8/30/2020` Int, `8/31/2020` Int, `9/1/2020` Int, `9/2/2020` Int, " +
    "`9/3/2020` Int, `9/4/2020` Int, `9/5/2020` Int, `9/6/2020` Int, `9/7/2020` Int, `9/8/2020` Int, `9/9/2020` Int, " +
    "`9/10/2020` Int, `9/11/2020` Int, `9/12/2020` Int, `9/13/2020` Int, `9/14/2020` Int, `9/15/2020` Int, `9/16/2020` Int, " +
    "`9/17/2020` Int, `9/18/2020` Int, `9/19/2020` Int, `9/20/2020` Int, `9/21/2020` Int, `9/22/2020` Int, `9/23/2020` Int, " +
    "`9/24/2020` Int, `9/25/2020` Int, `9/26/2020` Int, `9/27/2020` Int, `9/28/2020` Int, `9/29/2020` Int, `9/30/2020` Int, " +
    "`10/1/2020` Int, `10/2/2020` Int, `10/3/2020` Int, `10/4/2020` Int, `10/5/2020` Int, `10/6/2020` Int, `10/7/2020` Int, " +
    "`10/8/2020` Int, `10/9/2020` Int, `10/10/2020` Int, `10/11/2020` Int, `10/12/2020` Int, `10/13/2020` Int, " +
    "`10/14/2020` Int, `10/15/2020` Int, `10/16/2020` Int, `10/17/2020` Int, `10/18/2020` Int, `10/19/2020` Int, " +
    "`10/20/2020` Int, `10/21/2020` Int, `10/22/2020` Int, `10/23/2020` Int, `10/24/2020` Int, `10/25/2020` Int, " +
    "`10/26/2020` Int, `10/27/2020` Int, `10/28/2020` Int, `10/29/2020` Int, `10/30/2020` Int, `10/31/2020` Int, " +
    "`11/1/2020` Int, `11/2/2020` Int, `11/3/2020` Int, `11/4/2020` Int, `11/5/2020` Int, `11/6/2020` Int, `11/7/2020` Int, " +
    "`11/8/2020` Int, `11/9/2020` Int, `11/10/2020` Int, `11/11/2020` Int, `11/12/2020` Int, `11/13/2020` Int, " +
    "`11/14/2020` Int, `11/15/2020` Int, `11/16/2020` Int, `11/17/2020` Int, `11/18/2020` Int, `11/19/2020` Int, " +
    "`11/20/2020` Int, `11/21/2020` Int, `11/22/2020` Int, `11/23/2020` Int, `11/24/2020` Int, `11/25/2020` Int, " +
    "`11/26/2020` Int, `11/27/2020` Int, `11/28/2020` Int, `11/29/2020` Int, `11/30/2020` Int, `12/1/2020` Int, " +
    "`12/2/2020` Int, `12/3/2020` Int, `12/4/2020` Int, `12/5/2020` Int, `12/6/2020` Int, `12/7/2020` Int, `12/8/2020` Int, " +
    "`12/9/2020` Int, `12/10/2020` Int, `12/11/2020` Int, `12/12/2020` Int, `12/13/2020` Int, `12/14/2020` Int, " +
    "`12/15/2020` Int, `12/16/2020` Int, `12/17/2020` Int, `12/18/2020` Int, `12/19/2020` Int, `12/20/2020` Int, " +
    "`12/21/2020` Int, `12/22/2020` Int, `12/23/2020` Int, `12/24/2020` Int, `12/25/2020` Int, `12/26/2020` Int, " +
    "`12/27/2020` Int, `12/28/2020` Int, `12/29/2020` Int, `12/30/2020` Int, `12/31/2020` Int, `1/1/2021` Int, `1/2/2021` Int, " +
    "`1/3/2021` Int, `1/4/2021` Int, `1/5/2021` Int, `1/6/2021` Int, `1/7/2021` Int, `1/8/2021` Int, `1/9/2021` Int, " +
    "`1/10/2021` Int, `1/11/2021` Int, `1/12/2021` Int, `1/13/2021` Int, `1/14/2021` Int, `1/15/2021` Int, `1/16/2021` Int, " +
    "`1/17/2021` Int, `1/18/2021` Int, `1/19/2021` Int, `1/20/2021` Int, `1/21/2021` Int, `1/22/2021` Int, `1/23/2021` Int, " +
    "`1/24/2021` Int, `1/25/2021` Int, `1/26/2021` Int, `1/27/2021` Int, `1/28/2021` Int, `1/29/2021` Int, `1/30/2021` Int, " +
    "`1/31/2021` Int, `2/1/2021` Int, `2/2/2021` Int, `2/3/2021` Int, `2/4/2021` Int, `2/5/2021` Int, `2/6/2021` Int, " +
    "`2/7/2021` Int, `2/8/2021` Int, `2/9/2021` Int, `2/10/2021` Int, `2/11/2021` Int, `2/12/2021` Int, `2/13/2021` Int, " +
    "`2/14/2021` Int, `2/15/2021` Int, `2/16/2021` Int, `2/17/2021` Int, `2/18/2021` Int, `2/19/2021` Int, `2/20/2021` Int, " +
    "`2/21/2021` Int, `2/22/2021` Int, `2/23/2021` Int, `2/24/2021` Int, `2/25/2021` Int, `2/26/2021` Int, `2/27/2021` Int, " +
    "`2/28/2021` Int, `3/1/2021` Int, `3/2/2021` Int, `3/3/2021` Int, `3/4/2021` Int, `3/5/2021` Int, `3/6/2021` Int, " +
    "`3/7/2021` Int, `3/8/2021` Int, `3/9/2021` Int, `3/10/2021` Int, `3/11/2021` Int, `3/12/2021` Int, `3/13/2021` Int, " +
    "`3/14/2021` Int, `3/15/2021` Int, `3/16/2021` Int, `3/17/2021` Int, `3/18/2021` Int, `3/19/2021` Int, `3/20/2021` Int, " +
    "`3/21/2021` Int, `3/22/2021` Int, `3/23/2021` Int, `3/24/2021` Int, `3/25/2021` Int, `3/26/2021` Int, `3/27/2021` Int, " +
    "`3/28/2021` Int, `3/29/2021` Int, `3/30/2021` Int, `3/31/2021` Int, `4/1/2021` Int, `4/2/2021` Int, `4/3/2021` Int, " +
    "`4/4/2021` Int, `4/5/2021` Int, `4/6/2021` Int, `4/7/2021` Int, `4/8/2021` Int, `4/9/2021` Int, `4/10/2021` Int, " +
    "`4/11/2021` Int, `4/12/2021` Int, `4/13/2021` Int, `4/14/2021` Int, `4/15/2021` Int, `4/16/2021` Int, `4/17/2021` Int, " +
    "`4/18/2021` Int, `4/19/2021` Int, `4/20/2021` Int, `4/21/2021` Int, `4/22/2021` Int, `4/23/2021` Int, `4/24/2021` Int, " +
    "`4/25/2021` Int, `4/26/2021` Int, `4/27/2021` Int, `4/28/2021` Int, `4/29/2021` Int, `4/30/2021` Int, `5/1/2021` Int, " +
    "`5/2/2021` Int) row format delimited fields terminated by ','")
  spark.sql("LOAD DATA LOCAL INPATH 'time_series_covid_19_confirmed_US.csv' INTO TABLE CovConUS")

  spark.sql("SELECT * FROM CovConUS").show()
  queryMenu()

//the query menu is where the user can select what they want to do
  def queryMenu(): Unit = {

    println("COVID-19 Data Analysis\n")
    println("[1] Show covid confirmed data for the countries\n")
    println("[2] Show covid confirmed data for your state\n")
    println("[3] Show covid confirmed data for your city\n")
    println("[4] Show covid death data for the countries\n")
    println("[5] Show covid death data for your state\n")
    println("[6] Show covid death data for your city\n")
    println("[7] Show covid recovered data for the countries\n")
    println("[8] Logout\n")
    val user_input = readLine("Enter your selection: \n")
    user_input match {
      case "1" => {
        query1()
      }
      case "2" => {
        query2()
      }
      case "3" => {
        query3()
      }
      case "4" => {
        query4()
      }
      case "5" => {
        query5()
      }
      case "6" => {
        println("Logging out")
        queryMenu()
      }
      case "7" => {
        query7()
      }
      case "8" => {
        println("Logging out")
        sys.exit(0)
      }
      case _ => {
        println("Invalid input")
        queryMenu()
      }
    }
  }

  def query1(): Unit = {
    println("Enter the country name: ")
    val country = readLine().toLowerCase.capitalize
    spark.sql(s"SELECT * FROM <tablename with confirmed data> WHERE country = '$country'").show() // need to enter the table name
    queryMenu()
  }

  def query2(): Unit = {
    println("Enter your state: ")
    val state = readLine().toLowerCase.capitalize //need to resolve space issue with the toLowerCase.capitalize example "new york" returns empty
    spark.sql(s"SELECT * FROM CovConUS WHERE Province_State = '$state'").show()
    queryMenu()
  }

  def query3(): Unit = {
    println("Enter your city: ")
    val city = readLine().toLowerCase.capitalize
    spark.sql(s"SELECT * FROM CovConUS WHERE Admin2 = '$city'").show()
    queryMenu()
  }

  def query4(): Unit = {
    println("Enter your country name: ")
    val country = readLine().toLowerCase.capitalize
    spark.sql(s"SELECT * FROM <tablename with death data> WHERE country = '$country'").show() //need to enter the table name
    queryMenu()
  }

  def query5(): Unit = {
    println("Enter your state: ")
    val state = readLine().toLowerCase.capitalize
    spark.sql(s"SELECT * FROM <tablename with death data> WHERE city = '$state'").show() //need to enter table name
    queryMenu()
  }

  def query6(): Unit = {
    println("Enter your city name: ")
    val city = readLine().toLowerCase.capitalize
    spark.sql(s"SELECT * FROM <tablename with death data> WHERE city = '$city'").show() //need to enter table name
    queryMenu()
  }

  def query7(): Unit = {
    println("Enter your country name: ")
    val country = readLine().toLowerCase.capitalize
    spark.sql(s"SELECT * FROM <tablename with recovered data> WHERE country = '$country'").show() //need to enter table name
    queryMenu()
  }


}
