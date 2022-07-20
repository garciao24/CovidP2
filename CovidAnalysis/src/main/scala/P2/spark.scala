package P2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

class spark (){
  val spark = SparkSession
    .builder
    .appName("appName")
    .config("spark.master", "local[*]")
    .enableHiveSupport()
    .getOrCreate()
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("aka").setLevel(Level.OFF)
  //PropertyConfigurator.configure("log4j.properties")
  val logger: Logger = org.apache.log4j.Logger.getRootLogger()
  //println("created spark session")
  logger.info("Created Spark Session")
}
