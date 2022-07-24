package P2

import P2.Main.session
import P2.P2tempviews.{df2, df4}

import scala.io.StdIn.readLine

object P2queries{


  //Extraction
  //val df1 = session.spark.read.option("header", "true").csv("hdfs://localhost:9000/user/bobcat2k4/time_series_covid_19_confirmed_US.csv")
  //df1.show()
  //val df2 = session.spark.read.option("header", "true").csv("hdfs://localhost:9000/user/bobcat2k4/time_series_covid_19_deaths_US.csv")
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
    println("[1] Show covid confirmed data for the countries\n") //not functional
    println("[2] Show the total covid confirmed data for all the states\n") //works
    println("[3] Show the total covid confirmed data for top 20 cities\n") //works
    println("[4] Show covid death data for the countries\n") //not functional
    println("[5] Show the total covid death data for all the states\n") //works
    println("[6] Show the total covid death data for top 20 cities\n") //works
    println("[7] Show covid recovered data for the countries\n") //not functional
    println("[8] What is the covid confirmed percentage from state's total population?\n") //nee to resolve decimal places for percentage
    println("[9] What is the death percentage from state's total population?\n") //need to resolve decimal places for percentage
    println("[10] What is the case fatality rate for each state?\n") //need to resolve decimal places for percentage
    println("[11] Show the top 20 states with the highest case fatality rate\n") //need to resolve decimal places for percentage
    println("[12] Show the top 20 states with the lowest case fatality rate\n") //need to resolve decimal places for percentage
    //println("[13] What is the recovered percentage from state's total population?\n") //can't figure out recovery data using this dataset
    //println("[14] What is the recovery percentage from the number of total confirmed cases?\n") //can't figure out recovery data using this dataset
    println("[15] Logout\n")

    val user_input = readLine("Enter your selection: \n")
    user_input match {
      case "1" => {
        //query1()
        println("Not functional\n")
        queryMenu()
      }
      case "2" => {
        query2()
      }
      case "3" => {
        query3()
      }
      case "4" => {
        //query4()
        println("Not functional\n")
        queryMenu()
      }
      case "5" => {
        query5()
      }
      case "6" => {
        query6()
      }
      case "7" => {
        //query7()
        println("Not functional\n")
        queryMenu()
      }
      case "8" => {
        query8()
      }
      case "9" => {
        query9()
      }
      case "10" => {
        query10()
      }
      case "11" => {
        query11()
      }
      case "12" => {
        query12()
      }
      case "13" => {
        //query13()
        println("Not functional\n")
        queryMenu()
      }
      case "14" => {
        //query14()
        println("Not functional\n")
        queryMenu()
      }
      case "15" => {
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
    println("Enter the country name: ")
    val country = readLine().toLowerCase.capitalize
    session.spark.sql(s"SELECT * FROM <tablename with confirmed data> WHERE country = '$country'").show() // need to enter the table name
    queryMenu()
  }

  def query2(): Unit = {
    println("Showing Results for all the states")
    session.spark.sql("SELECT Province_State AS State, SUM(`5/2/21`) AS Total_Confirmed FROM CovConUS GROUP BY State ORDER BY Total_Confirmed DESC").show(100)
    queryMenu()
  }

  def query3(): Unit = {
    println("Showing Results for top 20 cities")
    session.spark.sql("SELECT Admin2 AS City, Province_State AS State, SUM(`5/2/21`) AS Total_Confirmed FROM CovConUS GROUP BY City, State ORDER BY Total_Confirmed DESC").show(20)
    queryMenu()
  }

  def query4(): Unit = {
    println("Enter your country name: ")
    val country = readLine().toLowerCase.capitalize
    session.spark.sql(s"SELECT * FROM <tablename with death data> WHERE country = '$country'").show() //need to enter the table name
    queryMenu()
  }

  def query5(): Unit = {
    println("Showing Results for all the states")
    session.spark.sql("SELECT Province_State AS State, SUM(`5/2/21`) AS Total_Death_Confirmed FROM CovDeathUS GROUP BY State ORDER BY Total_Death_Confirmed DESC").show(100)
    queryMenu()
  }

  def query6(): Unit = {
    println("Showing Results for top 20 cities")
    session.spark.sql("SELECT Admin2 AS City, Province_State AS State, SUM(`5/2/21`) AS Total_Death_Confirmed FROM CovDeathUS GROUP BY City, State ORDER BY Total_Death_Confirmed DESC").show(20)
    queryMenu()
  }

  def query7(): Unit = {
    println("Enter your country name: ")
    val country = readLine().toLowerCase.capitalize
    session.spark.sql(s"SELECT * FROM <tablename with recovered data> WHERE country = '$country'").show() //need to enter table name
    queryMenu()
  }

  def query8(): Unit ={
    println("Enter your state: ")
    val state = readLine()//.toLowerCase.capitalize
    //spark.sql(s"SELECT SUM(`5/2/21`) AS Total_Confirmed FROM CovConUS WHERE Province_State = '$state'").show() //Shows the total confirmed
    //spark.sql(s"SELECT SUM(Population) AS Total_Population FROM CovDeathUS WHERE Province_State = '$state'").show() //Shows the total population
    session.spark.sql(s"SELECT CovConUS.Province_State AS State, SUM(CovConUS.`5/2/21`)*100/SUM(CovDeathUS.Population) AS Covid_Confirmed_Percentage FROM CovConUS, CovDeathUS WHERE CovConUS.Province_State = '$state' AND CovDeathUS.Province_State = '$state' Group BY State").show()
    queryMenu()
  }

  def query9(): Unit = {
    println("Enter your state: ")
    val state = readLine()//.toLowerCase.capitalize //need to resolve space issue with the toLowerCase.capitalize example "new york" returns empty
    //spark.sql(s"SELECT SUM(`5/2/21`) AS Total_Death, SUM(Population) AS Total_State_Population, (SUM(`5/2/21`)*100/SUM(Population)) AS Death_Percentage FROM CovDeathUS WHERE Province_State = '$state'").show() //shows total death, total population, and death percentage
    session.spark.sql(s"SELECT Province_State AS State, SUM(`5/2/21`)*100/SUM(Population) AS Covid_Death_Percentage FROM CovDeathUS WHERE Province_State = '$state' Group BY State").show()
    queryMenu()
  }

  def query10(): Unit = {
    println("Enter your state: ")
    val state = readLine()//.toLowerCase.capitalize
    //formula for case fatality rate = (total death/total confirmed) * 100
    //spark.sql(s"SELECT SUM(`5/2/21`) AS Total_Death FROM CovDeathUS WHERE Province_State = '$state'").show() //shows total death
    //spark.sql(s"SELECT SUM(`5/2/21`) AS Total_Confirmed FROM CovConUS WHERE Province_State = '$state'").show() //shows total confirmed
    session.spark.sql(s"SELECT CovDeathUS.Province_State AS State, SUM(CovDeathUS.`5/2/21`)/SUM(CovConUS.`5/2/21`)*100 AS Case_Fatality_Rate FROM CovConUS, CovDeathUS WHERE CovDeathUS.Province_State = '$state' AND CovConUS.Province_State = '$state' GROUP BY State").show()
    queryMenu()
  }

  def query11(): Unit = {
    println("Showing results for the top 20 states with the highest case fatality rates")
    //formula for case fatality rate = (total death/total confirmed) * 100
    session.spark.sql("SELECT CovDeathUS.Province_State AS State, SUM(CovDeathUS.`5/2/21`)/SUM(CovConUS.`5/2/21`)*100 AS Case_Fatality_Rate FROM CovConUS, CovDeathUS GROUP BY State ORDER BY Case_Fatality_Rate DESC").show(20)
    queryMenu()
  }

  def query12(): Unit = {
    println("Showing results for the top 20 states with the lowest case fatality rates")
    //formula for case fatality rate = (total death/total confirmed) * 100
    session.spark.sql("SELECT CovDeathUS.Province_State AS State, SUM(CovDeathUS.`5/2/21`)/SUM(CovConUS.`5/2/21`)*100 AS Case_Fatality_Rate FROM CovConUS, CovDeathUS GROUP BY State ORDER BY Case_Fatality_Rate").show(20)
    queryMenu()
  }



}
