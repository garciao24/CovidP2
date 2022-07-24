package P2

import P2.Main.session
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

//Current path is C:\inputs\inputproject2
object P2functions {


  var export1: DataFrame = _
  var export2: DataFrame = _
  var export3: DataFrame = _

  //===================================================================================================== Connects to file.
  def connectlink(): Unit ={
    println(Console.GREEN + "Status----------------->Connected" + Console.RESET)
  }
  //----------------------------------------------------------------------------------------------------- Total confirmed by state.
  def totalbystates(v1:Boolean,v2:Boolean)={
    session.logger.info("Total confirmed by state.")
    export1 = session.spark.sql("SELECT Province_State, SUM(`5/1/21`) AS DatedTotal " +
      "FROM CovConUS GROUP BY Province_State ORDER BY DatedTotal DESC")//.show(10000,false)

    if(v1){
      export1.show(200)
    }
    if(v2){
      file.outputJson("TotalUSStateCases",export1)
      file.outputcsv("TotalUSStateCases",export1)
    }
  }
  //----------------------------------------------------------------------------------------------------- Input two dates get new cases.
  def datedchange()={
    var initialdate = "`4/30/21`" //scala.io.StdIn.readLine()
    var finaldate = "`5/1/21`"  //scala.io.StdIn.readLine()
    session.logger.info(f"New cases based on new cases from $initialdate to $finaldate")

    session.spark.sql(f"SELECT Province_State, SUM($finaldate)-SUM($initialdate) AS NewCases " +
      "FROM CovConUS GROUP BY Province_State ORDER BY NewCases DESC").show(10000,false)
  }
  //----------------------------------------------------------------------------------------------------- Total confirmed in a day.
  def totalday()= {
    var stringeddate = "`5/2/21`" //scala.io.StdIn.readLine()
    println(f"Total for $stringeddate")
    session.spark.sql(f"SELECT Province_State, SUM($stringeddate) AS Ratio " +
      "FROM CovConUS GROUP BY Province_State ORDER BY Ratio DESC").show(10000,false)
  }
  //----------------------------------------------------------------------------------------------------- Total new confirmed in a month.
  def monthrange(v1:Boolean,v2:Boolean) = {
    var mnth = 4  //scala.io.StdIn.readLine()
    var yr = "21" //scala.io.StdIn.readLine()
    var monthimp = f"'$mnth/"
    var yearimp = f"$yr`"
    var finalimpstring = ""
    var x = 0
    var firstimp = ""

    // Save table into variable, gets header, makes string, then makes list.
    var chosendf = "CovConUS" //scala.io.StdIn.readLine()
    var currdf = session.spark.sql(f"SELECT * FROM $chosendf")
    var ColumnNames=currdf.columns
    var Columnstring = ColumnNames.mkString("`", "`,`", "`")
    var Columnlist = Columnstring.split(",")

    session.logger.info(s"Monthly Deaths for $monthimp" + f"th month of 20$yearimp")

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
    export2 = session.spark.sql(f"SELECT Province_State, SUM($finalimpstring) AS April_Total " +
      "FROM CovConUS GROUP BY Province_State ORDER BY April_Total DESC")//.show(10000,false)

    //println(finalimpstring)
    //println(listofdates.length)

    if(v1){
      export2.show(200)
    }
    if(v2){
      file.outputJson("SpecificMonthCasesByState",export2)
      file.outputcsv("SpecificMonthCasesByState",export2)
    }

  }

  def daily(v1:Boolean,v2:Boolean): Unit ={ // Subtract current day minus previous day, for first day just print that day. Show all deaths worldwide by country.
    var chosendf = "CovDeaths" //scala.io.StdIn.readLine()
    var currdf = session.spark.sql(f"SELECT * FROM $chosendf")
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
    session.logger.info("Daily Deaths From Covid Worldwide")
    export3 = session.spark.sql(f"SELECT `Country/Region`, $finalimpstring " +
      "FROM CovDeaths GROUP BY `Country/Region` ORDER BY `Country/Region` DESC")//.show(10000,false)

    if(v1){
      export3.show(200)
    }
    if(v2){
      file.outputJson("DailyNewDeathsByCountry",export3)
      file.outputcsv("DailyNewDeathsByCountry",export3)
    }

  }

  //connectlink()park
  //datedchange()
  //totalday()

//  totalbystates(true,false) //<------------------ This one for Project 2
//
//  monthrange(true,false) //<------------------ This one for Project 2
//
//
//  daily(true,false) //<------------------ This one for Project 2
//
//
//
//  session.spark.sql("SHOW DATABASES").show()
//  session.spark.sql("SHOW TABLES").show()


}
