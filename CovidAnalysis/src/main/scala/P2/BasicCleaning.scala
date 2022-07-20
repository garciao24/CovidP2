package P2
//import P2.Main.my_logger
import P2.Main.session
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object BasicCleaning {
  private var df: DataFrame = _
 def runOscar():Unit={

   //my_logger.info("loading from a file!!!! ")


   df = session.spark.read.option("delimiter", ",").option("inferSchema","true").option("header", "true").csv("C:\\Users\\wolf1\\Desktop\\Provisional_COVID-19_Deaths_by_Sex_and_Age.csv")
   session.logger.info("loading from a ---- setting dataframes ")
   session.logger.info("getting all USA Deaths by State")

   q1()
   session.logger.info("getting deaths by age group")
   q2()
   session.logger.info("getting deaths per month from 2020 January to present")
   q3()
 }

  def q1():Unit={

    session.logger.info("Extracting data")
    val df1 = df.select("State","COVID-19 Deaths")
      .filter(df("Sex") === "All Sexes" && df("Group") === "By Total" &&
        df("Age Group") === "All Ages" &&
        df("State") =!= "United States")


    file.outputJson("totalUSA_State",df1)
    file.outputcsv("totalUSA_State",df1)
  }

  def q2():Unit={


    session.logger.info("Extracting data")
    val df2 = df.select("Age Group","COVID-19 Deaths")
      .filter(df("Sex") === "All Sexes" && df("Group") === "By Total" &&
        df("State") === "United States" && df("Age Group") =!= "All Ages"
        && df("Age Group") =!= "0-17 years")


    file.outputJson("death_by_ageGroup",df2)
    file.outputcsv("death_by_ageGroup",df2)
  }

  def q3():Unit={

    session.logger.info("Extracting data")
    var df3 = df.groupBy("Year", "Month").agg(sum("COVID-19 Deaths").as("Total Covid Deaths"))
      .filter(df("Month").isNotNull).orderBy(df("Year").asc, df("Month").asc)

    val getConcatenated = udf( (first: String, second: String) => { first + "_" + second } )
    df3 = df3.withColumn("Date", getConcatenated(df3("Year"), df3("Month"))).select("Date", "Total Covid Deaths")

    df3 = df3.filter(df3("Date") =!= "2022_7")

      file.outputJson("death_by_month",df3)
      file.outputcsv("death_by_month",df3)
    }


}
