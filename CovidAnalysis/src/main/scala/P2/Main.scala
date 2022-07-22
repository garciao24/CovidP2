package P2

object Main {

  val session = new spark()
  var currentUser:String = _
  def main(args: Array[String]): Unit = {
    currentUser = "ogarcia2834"

    session.logger.info("test")/////usage of logger example
    //BasicCleaning.runOscar()

    Queries.createTablesE()






  }
}