package P2



import scala.io.StdIn.readLine
import scala.sys.exit

object Main {
  val bucket = "d6p2"//hardcode AWS bucket name
  val session = new spark()
  def main(args: Array[String]): Unit = {

    session.logger.info("test")/////usage of logger example
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
      case "1" => P2queries.query1(v1 = true,v2 = true)
      case "2" => P2queries.query2(v1 = true,v2 = true)
      case "3" => P2queries.query3(v1 = true,v2 = true)
      case "4" => P2queries.query4(v1 = true,v2 = true)
      case "5" => P2functions.totalbystates(v1 = true,v2 = true)
      case "6" => P2functions.monthrange(v1 = true,v2 = true)
      case "7" => P2functions.daily(v1 = true,v2 = true)
      case "8" => CovidP2Thuva.UsDeathDataByMonth(v1 = true,v2 = true)
      case "9" => CovidP2Thuva.DeathVSRecoverPercentage(v1 = true,v2 = true)
      case "10" => CovidP2Thuva.TopDays(v1 = true,v2 = true)
      case "11" => Queries.query1(v1 = true,v2 = true)
      case "12" => Queries.query2(v1 = true,v2 = true)
      case "13" => Queries.query3(v1 = true,v2 = true)
      case "14" => BasicCleaning.q1(v1 = true,v2 = true)
      case "15" => BasicCleaning.q2(v1 = true,v2 = true)
      case "16" => BasicCleaning.q3(v1 = true,v2 = true)
      case "17" => choice()
      case _ => {
        println("Invalid input")
        userMenu()
      }
    }
    userMenu()
  }



  def debug(): Unit = {

    P2queries.query1(v1 = true,v2 = false)
    P2queries.query2(v1 = true,v2 = false)
    P2queries.query3(v1 = true,v2 = false)
    P2queries.query4(v1 = true,v2 = false)
    P2functions.totalbystates(v1 = true,v2 = false)
    P2functions.monthrange(v1 = true,v2 = false)
    P2functions.daily(v1 = true,v2 = false)
    CovidP2Thuva.UsDeathDataByMonth(v1 = true,v2 = false)
    CovidP2Thuva.DeathVSRecoverPercentage(v1 = true,v2 = false)
    CovidP2Thuva.TopDays(v1 = true,v2 = false)
    Queries.query1(v1 = true,v2 = false)
    Queries.query2(v1 = true,v2 = false)
    Queries.query3(v1 = true,v2 = false)
    BasicCleaning.q1(v1 = true,v2 = false)
    BasicCleaning.q2(v1 = true,v2 = false)
    BasicCleaning.q3(v1 = true,v2 = false)
  }

  def export(): Unit = {


    P2queries.query1(v1 = false,v2 = true)
    P2queries.query2(v1 = false,v2 = true)
    P2queries.query3(v1 = false,v2 = true)
    P2queries.query4(v1 = false,v2 = true)
    P2functions.totalbystates(v1 = false,v2 = true)
    P2functions.monthrange(v1 = false,v2 = true)
    P2functions.daily(v1 = false,v2 = true)
    CovidP2Thuva.UsDeathDataByMonth(v1 = false,v2 = true)
    CovidP2Thuva.DeathVSRecoverPercentage(v1 = false,v2 = true)
    CovidP2Thuva.TopDays(v1 = false,v2 = true)
    Queries.query1(v1 = false,v2 = true)
    Queries.query2(v1 = false,v2 = true)
    Queries.query3(v1 = false,v2 = true)
    BasicCleaning.q1(v1 = false,v2 = true)
    BasicCleaning.q2(v1 = false,v2 = true)
    BasicCleaning.q3(v1 = false,v2 = true)
  }

}