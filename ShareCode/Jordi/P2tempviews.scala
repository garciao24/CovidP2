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

    val df1 = spark.read.format("csv").option("header","true").load("hdfs://localhost:9000/user/hive/warehouse/Project2/time_series_covid_19_confirmed_US.csv") // File location in hdfs
    df1.createOrReplaceTempView("CovConUSImp")   //Temp table name given     //Type didn't seem to matter, just loaded straight .txt that looked like tables.

    spark.sql("SELECT * FROM CovConUSImp;").show()                     // Show() displays current tables.

    val df2 = spark.read.format("csv").option("header","true").load("hdfs://localhost:9000/user/hive/warehouse/Project2/time_series_covid_19_confirmed.csv") // File location in hdfs
    df2.createOrReplaceTempView("CovConImp")

    df1.write.mode("overwrite").saveAsTable("CovConUS2") // Basicall all that is needed, must be in same input file as all project.
    spark.sql("SELECT * FROM CovConUS2").show()

    df2.write.mode("overwrite").saveAsTable("CovCon2") // Basicall all that is needed, must be in same input file as all project.
    spark.sql("SELECT * FROM CovCon2").show()

    spark.sql("SHOW DATABASES").show()
    spark.sql("SHOW TABLES").show()

  }
}