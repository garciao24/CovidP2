import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._

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
  def mortalityration()= {
    //Divide sum of deaths each day by sum of cases each day. (Instead of sum you could do new cases.) New cases vs new deaths.
    println("df1 Query")
    var df = spark.sql("SELECT * FROM CovConUS WHERE ").show()
    println("df2 Query")
    /*
    var df2 = spark.sql("SELECT Province_State," +
      "SUM('1/23/2020') OVER(PARTITION BY Province_State ORDER BY Province_State) AS SumColumn, " +
      "SUM('1/23/2020') OVER(PARTITION BY Province_State) AS TotalColumn " +
      "FROM CovConUS")
      df2.show(100,false)*/
    println("df3 Query")
    var df3 = spark.sql("SELECT Province_State, SUM(`6/23/2020`) AS Sumation FROM CovConUS GROUP BY Province_State").show(100,false)

    println("df4 Query")
    var df4 = spark.sql("SELECT Province_State, SUM(`6/23/2020`) AS Sumation FROM CovConUS GROUP BY Province_State").show(100,false)

  }

  mortalityration()

}

