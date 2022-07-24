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
    println("[1] Show the total covid confirmed data for top 20 cities\n")
    println("[2] Show top 20 highest covid confirmed percentage from state's total population\n")
    println("[3] Show the top 20 states with the highest case fatality rate\n")
    println("[4] Show the top 20 states with the lowest case fatality rate\n")
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
    println("Showing Results for top 20 states")
    spark.sql("SELECT c.Province_State AS State, Round(SUM(c.`5/2/21`)*100/sum(d.Population),2) AS Covid_Confirmed_Percent FROM CovConUS c JOIN CovDeathUS d ON c.UID = d.UID Group By State Order By Covid_Confirmed_Percent DESC").show(20)
    queryMenu()
  }

  def query3(): Unit = {
    println("Showing Results for top 20 states with the highest case fatality rate")
    //formula for case fatality rate = (total death/total confirmed) * 100
    spark.sql("SELECT d.Province_State AS State, ROUND((SUM(d.`5/2/21`)/SUM(c.`5/2/21`)*100),2) AS Case_Fatality_Rate FROM CovDeathUS d JOIN CovConUS c ON d.UID = c.UID GROUP BY State ORDER BY Case_Fatality_Rate DESC").show(20)
    queryMenu()
  }

  def query4(): Unit = {
    println("Showing results for the top 20 states with the lowest case fatality rates")
    //formula for case fatality rate = (total death/total confirmed) * 100
    spark.sql("SELECT d.Province_State AS State, ROUND((SUM(d.`5/2/21`)/SUM(c.`5/2/21`)*100),2) AS Case_Fatality_Rate FROM CovDeathUS d JOIN CovConUS c ON d.UID = c.UID GROUP BY State ORDER BY Case_Fatality_Rate").show(20)
    queryMenu()
  }


}