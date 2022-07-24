package P2

import P2.Main.session
import P2.P2tempviews.{df1, df2, df4, df5}

import scala.io.StdIn.readLine

object P2queries{


  //Extraction
  //val df1 = spark.read.option("header", "true").csv("hdfs://localhost:9000/user/bobcat2k4/time_series_covid_19_confirmed_US.csv")
  //df1.show()
  //val df2 = spark.read.option("header", "true").csv("hdfs://localhost:9000/user/bobcat2k4/time_series_covid_19_deaths_US.csv")
  //df2.show()
  //Transformation
  val df3 = df2.drop("Combined_Key")
  df3.createOrReplaceTempView("CovConUS")
  //df3.show()
  val df = df4.drop("Combined_Key")
  df.createOrReplaceTempView("CovDeathUS")
  //df4.show()
  queryMenu()


//the query menu is where the user can select what they want to do
  def queryMenu(): Unit = {

    println("COVID-19 Data Analysis\n")
    println("[1] Show the total covid confirmed data for top 20 cities\n") //works
    println("[2] What is the covid confirmed percentage from state's total population?\n") //nee to resolve decimal places for percentage
    println("[3] What is the case fatality rate for each state?\n") //need to resolve decimal places for percentage
    println("[4] Show the top 20 states with the highest case fatality rate\n") //if we can fix the result's accuracy, if not discard it
    println("[5] Logout\n")

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
    println("Showing Results for top 20 cities")
    spark.sql("SELECT Admin2 AS City, Province_State AS State, SUM(`5/2/21`) AS Total_Confirmed FROM CovConUS GROUP BY City, State ORDER BY Total_Confirmed DESC").show(20)
    queryMenu()
  }

  def query2(): Unit ={
    println("Enter your state: ")
    val state = readLine()//.toLowerCase.capitalize
    //spark.sql(s"SELECT SUM(`5/2/21`) AS Total_Confirmed FROM CovConUS WHERE Province_State = '$state'").show() //Shows the total confirmed
    //spark.sql(s"SELECT SUM(Population) AS Total_Population FROM CovDeathUS WHERE Province_State = '$state'").show() //Shows the total population
    spark.sql(s"SELECT CovConUS.Province_State AS State, SUM(CovConUS.`5/2/21`)*100/SUM(CovDeathUS.Population) AS Covid_Confirmed_Percentage FROM CovConUS, CovDeathUS WHERE CovConUS.Province_State = '$state' AND CovDeathUS.Province_State = '$state' Group BY State").show()
    queryMenu()
  }

  def query3(): Unit = {
    println("Enter your state: ")
    val state = readLine()//.toLowerCase.capitalize
    //formula for case fatality rate = (total death/total confirmed) * 100
    //spark.sql(s"SELECT SUM(`5/2/21`) AS Total_Death FROM CovDeathUS WHERE Province_State = '$state'").show() //shows total death
    //spark.sql(s"SELECT SUM(`5/2/21`) AS Total_Confirmed FROM CovConUS WHERE Province_State = '$state'").show() //shows total confirmed
    spark.sql(s"SELECT CovDeathUS.Province_State AS State, SUM(CovDeathUS.`5/2/21`)/SUM(CovConUS.`5/2/21`)*100 AS Case_Fatality_Rate FROM CovConUS, CovDeathUS WHERE CovDeathUS.Province_State = '$state' AND CovConUS.Province_State = '$state' GROUP BY State").show()
    queryMenu()
  }

  def query4(): Unit = {
    println("Showing results for the top 20 states with the highest case fatality rates")
    //formula for case fatality rate = (total death/total confirmed) * 100
    spark.sql("SELECT CovDeathUS.Province_State AS State, SUM(CovDeathUS.`5/2/21`)/SUM(CovConUS.`5/2/21`)*100 AS Case_Fatality_Rate FROM CovConUS, CovDeathUS GROUP BY State ORDER BY Case_Fatality_Rate DESC").show(20)
    queryMenu()
  }


}
