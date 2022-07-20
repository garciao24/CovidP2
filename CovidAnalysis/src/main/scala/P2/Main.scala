package P2

object Main {

  val session = new spark()
  def main(args: Array[String]): Unit = {

    session.logger.info("test")/////usage of logger example
    BasicCleaning.runOscar()



  }
}