package P2



import scala.io.StdIn.readLine
import scala.sys.exit

object Main {

  val session = new spark()
  def main(args: Array[String]): Unit = {
    //session.logger.info("test")/////usage of logger example
    P2tempviews.CreateTemp()
    choice()
  }


  def choice(): Unit = {

    println {
      " \u001B[34mManual Menu\n" +
        "\u001B[32mOption 1: \u001B[34mManually search each query\n" +
        "\u001B[32mOption 2: \u001B[34mDebug(show all tables)\n" +
        "\u001B[32mOption 3: \u001B[34mExport Data\n"+
        "\u001B[32mOption 4: \u001B[34mexit\u001B[00m\n"

    }
    val input = scala.io.StdIn.readLine()
    input match {
      case "1" => userMenu()//manual
      case "2" => debug()//debug mode show tables
      case "3" => export()// export data
      case "4" => exit(0)
      case _ => choice()

    }
    choice()
  }



  def userMenu(): Unit = {
    ////joseph
    println("[1] Show the total covid confirmed data for top 20 cities")
    println("[2] Show top 20 highest covid confirmed percentage from state's total population")
    println("[3] Show the top 20 states with the highest case fatality rate")
    println("[4] Show the top 20 states with the lowest case fatality rate")
    ///////jordi
    println("[5] Total confirmed by state")
    println("[6] monthrange")
    println("[7] daily new cases")
    ///////thuvarkan
    println("[8] Sum of Covid Death Data By Month")
    println("[9] Top 10 Countries which has best Recovery/Death Percentage against Confirmed Cases..")
    println("[10] Load top 20 Days recorded highest Confirmed Cases between 2020 -2021")
    //////////////Edwin
    println("[11] How many people were recovered worldwide by the last quarter of 2020")
    println("[12] What are the top 10 cities with number of deaths in the US?")
    println("[13] What are the countries with most recovered covid cases during the pandemic?")
    //////Oscar
    println("[14] Getting all USA Deaths by State")
    println("[15] Getting deaths by age group")
    println("[16] getting deaths per month from 2020 January to present")
    println("[17] Go back")


    val user_input = readLine("Enter your selection: \n")
    user_input match {
      case "1" => P2queries.query1(true,true)
      case "2" => P2queries.query2(true,true)
      case "3" => P2queries.query3(true,true)
      case "4" => P2queries.query4(true,true)
      case "5" => P2functions.totalbystates(true,true)
      case "6" => P2functions.monthrange(true,true)
      case "7" => P2functions.daily(true,true)
      case "8" => CovidP2Thuva.UsDeathDataByMonth(true,true)
      case "9" => CovidP2Thuva.DeathVSRecoverPercentage(true,true)
      case "10" => CovidP2Thuva.TopDays(true,true)
      case "11" => Queries.query1(true,true)
      case "12" => Queries.query2(true,true)
      case "13" => Queries.query3(true,true)
      case "14" => BasicCleaning.q1(true,true)
      case "15" => BasicCleaning.q2(true,true)
      case "16" => BasicCleaning.q3(true,true)
      case "17" => choice()
      case _ => {
        println("Invalid input")
        userMenu()
      }
    }
    userMenu()
  }



  def debug(): Unit = {

    P2queries.query1(true,true)
    P2queries.query2(true,true)
    P2queries.query3(true,true)
    P2queries.query4(true,true)
    P2functions.totalbystates(true,true)
    P2functions.monthrange(true,true)
    P2functions.daily(true,true)
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


    P2queries.query1(false,true)
    P2queries.query2(false,true)
    P2queries.query3(false,true)
    P2queries.query4(false,true)
    P2functions.totalbystates(false,true)
    P2functions.monthrange(false,true)
    P2functions.daily(false,true)
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