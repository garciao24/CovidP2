package P2

import scala.io.StdIn.readLine
import scala.sys.exit

object Main {

  val session = new spark()
  def main(args: Array[String]): Unit = {

    session.logger.info("test")/////usage of logger example

    P2tempviews.CreateTemp()
    //Queries.createTablesE()

    //BasicCleaning.q1(true,false)




    //BasicCleaning.runOscar()
    //CovidP2Thuva.run()

    println("-----------------------------------------")
    //P2functions.connectlink()
    //P2queries.queryMenu()




  }


  def choice(): Unit = {
    val input = scala.io.StdIn.readLine()
    input match {
      case "1" => P2queries.queryMenu()//manual
      case "2" => debug()//debug mode show tables
      case "3" => export()// export data
      case "4" => exit(0)
      case _ => choice()

    }
    choice()
  }



  def userMenu(): Unit = {
    ////joseph
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
    println("[12] Show the top 20 states with the lowest case fatality rate\n")
    ///////thuvarkan
    println("[13] UsDeathDataByMonth?\n")
    println("[14] Top 10 Countries which has best Recovery/Death Percentage against Confirmed Cases..\n")
    println("[15] Death Percentage against Population in USA States\n")
    println("[16] Load top 20 Days recorded highest Confirmed Cases between 2020 -2021\n")
    ///////jordi
    println("[17] Total confirmed by state.?\n")
    println("[18] New cases based on new cases from 4/30/21 to 5/1/21\n")
    println("[19] Total for 5/2/21 \n")
    println("[20] Month range\n")
    //////////////Edwin
    println("[21] How many people were recovered worldwide by the last quarter of 2020\n")
    println("[22] What are the top 10 cities with number of deaths in the US?\n")
    println("[23] What are the countries with most recovered covid cases during the pandemic?\n")
    //////Oscar
    println("[24] Getting all USA Deaths by State\n")
    println("[25] Getting deaths by age group\n")
    println("[26] getting deaths per month from 2020 January to present\n")
    println("[27] Logout\n")


    val user_input = readLine("Enter your selection: \n")
    user_input match {
      case "1" => {


      }
      case "2" => {

      }
      case "3" => {
      }
      case "4" => {


      }
      case "5" => {

      }
      case "6" => {

      }
      case "7" => {


      }
      case "8" => {

      }
      case "9" => {

      }
      case "10" => {

      }
      case "11" => {

      }
      case "12" => {

      }
      case "13" => {


      }
      case "14" => {


      }
      case "20" => {
        println("Logging out")
        sys.exit(0)
      }
      case _ => {
        println("Invalid input")
        userMenu()
      }
    }


  }



  def debug(): Unit = {







  }

  def export(): Unit = {









  }






}