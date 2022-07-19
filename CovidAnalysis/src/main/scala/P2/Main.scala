package P2

import com.typesafe.scalalogging.Logger
import org.apache.log4j.PropertyConfigurator

object Main {

  def main(args: Array[String]): Unit = {


    PropertyConfigurator.configure("log4j.properties")//./log4j.properties

    val my_logger = Logger("name")
    // calling info method
    my_logger.info("Here we are using info method !!!! ")
    // calling debug method
    my_logger.debug("Here we are using debug method !!!! ")
    // calling error method
    my_logger.error("Here we are using error method !!!! ")
    println("logges been created. !!!")



  }

}
