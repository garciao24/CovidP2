package P2

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

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
    //spark.sql("SELECT * FROM UserInfo").show()
    }
    catch{
      case e: SQLException => e.printStackTrace()
    }
  }

  def showUsers(): Unit = {
    spark.sql("SELECT FirstName,LastName,Username FROM UserInfo").show()
  }

  def checkifExists(usercheck:String):Boolean = {
    val datalist = spark.sql("SELECT Username From UserInfo")
    val listOne = datalist.as(Encoders.STRING).collectAsList

    val check = listOne.contains(usercheck)

    if (check) {//user is detected
      true
    } else {//User is not detected
      false
    }
//    do {
//      println("Please enter a username: ")
//      userinput = scala.io.StdIn.readLine()
//      bool = listOne.contains(userinput)
//    }while(!bool)
  }


  def updatePassword(username:String, password: String): Unit = {
    try{
      spark.sql(f"UPDATE UserInfo SET = '$password' WHERE Username = '$username')")
    }
    catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def updateUsername(oldusername:String, NewUserName: String): Unit = {
    try{
      spark.sql(f"UPDATE UserInfo SET = '$NewUserName' WHERE Username = '$oldusername')")
    }
    catch {
      case e: Exception => e.printStackTrace()
    }




  }
}
