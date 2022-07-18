import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

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
