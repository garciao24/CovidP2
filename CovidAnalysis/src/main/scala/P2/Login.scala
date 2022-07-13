package P2

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.SQLException

object Login {
  private var bool : Boolean = false
  private var spark: SparkSession = _
  private var userDf: DataFrame = _
  private var df: DataFrame = _

  def connect(): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    spark = SparkSession
      .builder()
      .appName("Spark api")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session\n")

    spark.sparkContext.setLogLevel("ERROR")
    spark.sql("Set hive.exec.dynamic.partition.mode=nonstrict")
  }


  def createUser(FirstName: String, LastName: String, Username: String, Password: String, AdminPriv: Int): Unit = {

    try {
    spark.sql(f"INSERT INTO UserInfo(FirstName,LastName,Username, Password, AdminPriv) VALUES('$FirstName', '$LastName','$Username','$Password','$AdminPriv')")
    spark.sql("SELECT * FROM UserInfo").show()
    }
    catch{
      case e: SQLException => e.printStackTrace()
    }
  }

  //def show





}
