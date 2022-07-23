package P2

import P2.Main.session

//Current path is C:\inputs\inputproject2
object P2functions {
  def main(args: Array[String]): Unit = {

  } //Functions must be outside of the main function.

  //===================================================================================================== Connects to file.
  def connectlink(): Unit = {
    println(Console.GREEN + "Status----------------->Connected" + Console.RESET)
  }

  //----------------------------------------------------------------------------------------------------- Total confirmed by state.
  def totalbystates() = {
    println("Total confirmed by state.")
    session.spark.sql("SELECT Province_State, SUM(`5/1/21`) AS DatedTotal " +
      "FROM CovConUS GROUP BY Province_State ORDER BY DatedTotal DESC").show(100, false)
  }

  //----------------------------------------------------------------------------------------------------- Input two dates get new cases.
  def datedchange() = {
    var initialdate = "`4/30/21`" //scala.io.StdIn.readLine()
    var finaldate = "`5/1/21`" //scala.io.StdIn.readLine()
    println(f"New cases based on new cases from $initialdate to $finaldate")

    session.spark.sql(f"SELECT Province_State, SUM($finaldate)-SUM($initialdate) AS NewCases " +
      "FROM CovConUS GROUP BY Province_State ORDER BY NewCases DESC").show(100, false)
  }

  //----------------------------------------------------------------------------------------------------- Total confirmed in a day.
  def totalday() = {
    var stringeddate = "`5/2/21`" //scala.io.StdIn.readLine()
    println(f"Total for $stringeddate")
    session.spark.sql(f"SELECT Province_State, SUM($stringeddate) AS Ratio " +
      "FROM CovConUS GROUP BY Province_State ORDER BY Ratio DESC").show(100, false)
  }

  //----------------------------------------------------------------------------------------------------- Total new confirmed in a month.
  def monthrange() = {
    var mnth = 4 //scala.io.StdIn.readLine()
    var yr = "21" //scala.io.StdIn.readLine()
    var monthimp = f"'$mnth/"
    var yearimp = f"$yr`"
    var finalimpstring = ""
    var x = 0
    var firstiimp = ""

    // Save table into variable, gets header, makes string, then makes list.
    var chosendf = "CovConUS" //scala.io.StdIn.readLine()
    var currdf = session.spark.sql(f"SELECT * FROM $chosendf")
    var ColumnNames = currdf.columns
    var Columnstring = ColumnNames.mkString("`", "`,`", "`")
    var Columnlist = Columnstring.split(",")

    println(s"Monthly Deaths for $monthimp" + f"th month of 20$yearimp")

    Columnlist.foreach(i => {

      if (x == 0 && i.contains("`4/") && i.contains("21`")) { // Adds first value only.
        //println(f"$i Added at start.")
        finalimpstring += f"$i "
        firstiimp = i
        x = 1
      }

      else if (x == 1 && i.contains("`4/") && i.contains("/21`")) { // Adds value at the end.
        //println(f"$i Added at end.")
        finalimpstring = f"$i - $firstiimp"
      }

      else {
        //println(f" $i Not Found")
      }

    })
    session.spark.sql(f"SELECT Province_State, SUM($finalimpstring) AS April_Total " +
      "FROM CovConUS GROUP BY Province_State ORDER BY April_Total DESC").show(100, false)

    //println(finalimpstring)
    //println(listofdates.length)

  }
  /*
  def daily(): Unit ={ // Subtract current day minus previous day, for first day just print that day. Show all deaths worldwide by country.
    listofdates.foreach( i+1 =>

    spark.sql(f"SELECT Province_State, SUM($finalimpstring) AS April_Total " +
      "FROM CovConUS GROUP BY Province_State ORDER BY April_Total DESC").show(100,false)

  }*/

  //connectlink()
  //totalbystates()
  //datedchange()
  //totalday()
  monthrange()
  //dayrange()

  session.spark.sql("SHOW DATABASES").show()
  session.spark.sql("SHOW TABLES").show()

}
