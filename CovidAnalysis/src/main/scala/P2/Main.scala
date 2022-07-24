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

    export()
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
    println("[1] Show the total covid confirmed data for top 20 cities\n") //works
    println("[2] What is the covid confirmed percentage from state's total population?\n")
    println("[3] What is the case fatality rate for each state?\n")
    println("[4] Show the top 20 states with the highest case fatality rate\n")
    ///////jordi
    println("[17] Total confirmed by state.?\n")
    println("[18] New cases based on new cases from 4/30/21 to 5/1/21\n")
    println("[19] Total for 5/2/21 \n")
    println("[20] Month range\n")
    ///////thuvarkan
    println("[7] UsDeathDataByMonth?\n")
    println("[8] Top 10 Countries which has best Recovery/Death Percentage against Confirmed Cases..\n")
    println("[9] Load top 20 Days recorded highest Confirmed Cases between 2020 -2021\n")
    //////////////Edwin
    println("[10] How many people were recovered worldwide by the last quarter of 2020\n")
    println("[11] What are the top 10 cities with number of deaths in the US?\n")
    println("[12] What are the countries with most recovered covid cases during the pandemic?\n")
    //////Oscar
    println("[13] Getting all USA Deaths by State\n")
    println("[14] Getting deaths by age group\n")
    println("[15] getting deaths per month from 2020 January to present\n")
    println("[16] Logout\n")


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
      case "5" => {

      }
      case "7" => CovidP2Thuva.UsDeathDataByMonth(true,true)
      case "8" => CovidP2Thuva.DeathVSRecoverPercentage(true,true)
      case "9" => CovidP2Thuva.TopDays(true,true)
      case "10" => Queries.query1(true,true)
      case "11" => Queries.query2(true,true)
      case "12" => Queries.query3(true,true)
      case "13" => BasicCleaning.q1(true,true)
      case "14" => BasicCleaning.q2(true,true)
      case "15" => BasicCleaning.q3(true,true)

      case "16" => {
        choice()
      }
      case _ => {
        println("Invalid input")
        userMenu()
      }
    }


  }



  def debug(): Unit = {






    CovidP2Thuva.UsDeathDataByMonth(true,true)
    CovidP2Thuva.DeathVSRecoverPercentage(true,true)
    CovidP2Thuva.TopDays(true,true)
    Queries.query1(true,true)
    Queries.query2(true,true)
    Queries.query3(true,true)
    BasicCleaning.q1(true,true)
    BasicCleaning.q2(true,true)
    BasicCleaning.q3(true,true)
  }

  def export(): Unit = {






    CovidP2Thuva.UsDeathDataByMonth(false,true)
    CovidP2Thuva.DeathVSRecoverPercentage(false,true)
    CovidP2Thuva.TopDays(false,true)
    Queries.query1(false,true)
    Queries.query2(false,true)
    Queries.query3(false,true)
    BasicCleaning.q1(false,true)
    BasicCleaning.q2(false,true)
    BasicCleaning.q3(false,true)
  }

}