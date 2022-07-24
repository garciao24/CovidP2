import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

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

  var export1: DataFrame = _
  var export2: DataFrame = _
  var export3: DataFrame = _

  //===================================================================================================== Connects to file.
  def connectlink(): Unit ={
    println(Console.GREEN + "Status----------------->Connected" + Console.RESET)
  }
  //----------------------------------------------------------------------------------------------------- Total confirmed by state.
  def totalbystates()={
    println("Total confirmed by state.")
    export1 = spark.sql("SELECT Province_State, SUM(`5/1/21`) AS DatedTotal " +
      "FROM CovConUS GROUP BY Province_State ORDER BY DatedTotal DESC")//.show(10000,false)
  }
  //----------------------------------------------------------------------------------------------------- Input two dates get new cases.
  def datedchange()={
    var initialdate = "`4/30/21`" //scala.io.StdIn.readLine()
    var finaldate = "`5/1/21`"  //scala.io.StdIn.readLine()
    println(f"New cases based on new cases from $initialdate to $finaldate")

    spark.sql(f"SELECT Province_State, SUM($finaldate)-SUM($initialdate) AS NewCases " +
      "FROM CovConUS GROUP BY Province_State ORDER BY NewCases DESC").show(10000,false)
  }
  //----------------------------------------------------------------------------------------------------- Total confirmed in a day.
  def totalday()= {
    var stringeddate = "`5/2/21`" //scala.io.StdIn.readLine()
    println(f"Total for $stringeddate")
    spark.sql(f"SELECT Province_State, SUM($stringeddate) AS Ratio " +
      "FROM CovConUS GROUP BY Province_State ORDER BY Ratio DESC").show(10000,false)
  }
  //----------------------------------------------------------------------------------------------------- Total new confirmed in a month.
  def monthrange() = {
    var mnth = 4  //scala.io.StdIn.readLine()
    var yr = "21" //scala.io.StdIn.readLine()
    var monthimp = f"'$mnth/"
    var yearimp = f"$yr`"
    var finalimpstring = ""
    var x = 0
    var firstimp = ""

    // Save table into variable, gets header, makes string, then makes list.
    var chosendf = "CovConUS" //scala.io.StdIn.readLine()
    var currdf = spark.sql(f"SELECT * FROM $chosendf")
    var ColumnNames=currdf.columns
    var Columnstring = ColumnNames.mkString("`", "`,`", "`")
    var Columnlist = Columnstring.split(",")

    println(s"Monthly Deaths for $monthimp" + f"th month of 20$yearimp")

    Columnlist.foreach( i => {

      if (x == 0 && i.contains("`4/") && i.contains("21`")){  // Adds first value only.
        //println(f"$i Added at start.")
        finalimpstring += f"$i "
        firstimp = i
        x = 1
      }

      else if (x == 1 && i.contains("`4/") && i.contains("/21`")){ // Adds value at the end.
        //println(f"$i Added at end.")
        finalimpstring = f"$i - $firstimp"
      }

      else{
        //println(f" $i Not Found")
      }

    })
    export2 = spark.sql(f"SELECT Province_State, SUM($finalimpstring) AS April_Total " +
      "FROM CovConUS GROUP BY Province_State ORDER BY April_Total DESC")//.show(10000,false)

    //println(finalimpstring)
    //println(listofdates.length)

  }

  def daily(): Unit ={ // Subtract current day minus previous day, for first day just print that day. Show all deaths worldwide by country.
    var chosendf = "CovDeaths" //scala.io.StdIn.readLine()
    var currdf = spark.sql(f"SELECT * FROM $chosendf")
    var ColumnNames=currdf.columns
    var Columnstring = ColumnNames.mkString("`", "`,`", "`")
    var Columnlist = Columnstring.split(",")

    var finalimpstring = "SUM(`1/22/20`)"
    var x = 0
    //println(Columnlist.mkString(","))

    var firstimp = Columnlist.indexOf("`1/22/20`")
    var lastimp = Columnlist.length
    //println(firstimp)
    //println(lastimp)

    for ( i <- firstimp + 1 until lastimp){  //Until excludes last number, to includes it.
      var d1= Columnlist(i)
      var d2 = Columnlist(i-1)
      finalimpstring += f", Sum($d1 - $d2)"
      //(finalimpstring)

    }
    println("Daily Deaths From Covid Worldwide")
    export3 = spark.sql(f"SELECT `Country/Region`, $finalimpstring " +
      "FROM CovDeaths GROUP BY `Country/Region` ORDER BY `Country/Region` DESC")//.show(10000,false)

  }

  //connectlink()park
  //datedchange()
  //totalday()

  totalbystates() //<------------------ This one for Project 2
  file.outputJson("TotalUSStateCases",export1)
  file.outputcsv("TotalUSStateCases",export1)

  monthrange() //<------------------ This one for Project 2
  file.outputJson("SpecificMonthCasesByState",export2)
  file.outputcsv("SpecificMonthCasesByState",export2)

  daily() //<------------------ This one for Project 2
  file.outputJson("DailyNewDeathsByCountry",export3)
  file.outputcsv("DailyNewDeathsByCountry",export3)


  spark.sql("SHOW DATABASES").show()
  spark.sql("SHOW TABLES").show()


}

