import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

//Current path is C:\inputs\inputproject2
object P2functions {
  def main(args: Array[String]): Unit = {

  }  //Functions must be outside of the main function.
  System.setProperty("hadoop.home.dir", "C:\\hadoop")
  val spark = SparkSession
    .builder
    .appName("hello hive")
    .config("spark.master", "local[*]")
    .enableHiveSupport()
    .getOrCreate()
  Logger.getLogger("org").setLevel(Level.ERROR)
  println(Console.GREEN + "Status----------------->Spark Session Created" + Console.RESET)
  spark.sparkContext.setLogLevel("ERROR")
  //-------------------------------------------------------------------------------------------------------------------

  def connectlink(): Unit ={
    println(Console.GREEN + "Status----------------->Connected" + Console.RESET)
  }
  //-------------------------------------------------------------------------------------------------------------------
  def passwordmanagement(currentselect: String) ={//Extract password based on given username
    var extractpass = spark.sql(s"SELECT Password FROM UserInfo WHERE UserInfo.Username = '$currentselect'").first()

    var currentpass = extractpass(0)

    println("What is your password?")
    var askpass = scala.io.StdIn.readLine()

    if(currentpass == askpass ){
      println("Successfully Logged In")
      "Correct"
    }
    else{
      println("Wrong Password")
      "Wrong"}
  }

  def insertvalue(): Unit ={
    println("What Username Do You Wish To Add?")
    var usernameadding = scala.io.StdIn.readLine()
    println("What Is The New Password?")
    var passwordadding = scala.io.StdIn.readLine()
    spark.sql(f"INSERT INTO UserInfo(Username, Password) VALUES('$usernameadding', '$passwordadding')")
    spark.sql("SELECT * FROM UserInfo").show()
  }

  def resetvalues(): Unit ={
    println("What Username Do You Want To Delete?")
    var usernamedelete = scala.io.StdIn.readLine()
    spark.sql("DROP table IF EXISTS UserInfo")
    spark.sql("create table IF NOT EXISTS UserInfo(Username String, Password String) row format delimited fields terminated by ','")
    spark.sql("LOAD DATA LOCAL INPATH 'userinfo.csv' INTO TABLE UserInfo")
    spark.sql("SELECT * FROM UserInfo").show()
  }

  //-------------------------------------------------------------------------------------------------------------------
  def seetables()= {
    //println("What column do you wish to see?")
    var selection = "Neoplasms" //scala.io.StdIn.readLine()
    var yearselect = "2021" //scala.io.StdIn.readLine()
    spark.sql(f"SELECT Year, Month, Age, Race, $selection FROM MortalityDatabase WHERE MortalityDatabase.Year='$yearselect'").show(false) //Shows all select columns
  }
}
