package P2
//import P2.Main.my_logger
import P2.Main.{currentUser, session}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object BasicCleaning {
  private var df: DataFrame = _
  private var temp: DataFrame = _
  private var test: DataFrame = _
 def runOscar():Unit={

   df = session.spark.read.option("delimiter", ",").option("inferSchema","true").option("header", "true").csv(s"hdfs://localhost:9000/user/$currentUser/covidData/Provisional_COVID-19_Deaths_by_Sex_and_Age.csv")
   temp = session.spark.read.option("delimiter", ",").option("inferSchema","true").option("header", "true").csv(s"hdfs://localhost:9000/user/$currentUser/covidData/avg_tmp.csv")
   test = session.spark.read.option("delimiter", ",").option("inferSchema","true").option("header", "true").csv(s"hdfs://localhost:9000/user/$currentUser/covidData/covid_19_data.csv")

   session.logger.info("loading from a ---- setting dataframes ")
   session.logger.info("getting all USA Deaths by State")



   //q1()
   session.logger.info("getting deaths by age group")
   //q2()
   session.logger.info("getting deaths per month from 2020 January to present")
   //q3()

   q4()
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
    val selectCase = udf((tc: String) =>
      if (Seq("1").contains(tc)) "January"
      else if (Seq("2").contains(tc)) "February"
      else if (Seq("3").contains(tc)) "March"
      else if (Seq("4").contains(tc)) "April"
      else if (Seq("5").contains(tc)) "May"
      else if (Seq("6").contains(tc)) "June"
      else if (Seq("7").contains(tc)) "July"
      else if (Seq("8").contains(tc)) "August"
      else if (Seq("9").contains(tc)) "September"
      else if (Seq("10").contains(tc)) "October"
      else if (Seq("11").contains(tc)) "November"
      else if (Seq("12").contains(tc)) "December"
      else null
    )

    session.logger.info("Extracting data")
    var df3 = df.groupBy("Year", "Month").agg(sum("COVID-19 Deaths").as("Total Covid Deaths"))
      .filter(df("Month").isNotNull).orderBy(df("Year").asc, df("Month").asc)

    //df3 = df3.withColumn("Months",selectCase(df3("Month")))
    //df3.show()

    val getConcatenated = udf( (first: String, second: String) => { first + "_" + second } )
    df3 = df3.withColumn("Date", getConcatenated(df3("Year"), df3("Month"))).select("Date", "Total Covid Deaths")
    //df3.show()
    val fin = df3.filter(df3("Date") =!= "2022_7").join(temp, df3("Date") === temp("Date_month"), "left").select("Date","Total Covid Deaths","Avg_temp(F*)")

    //fin.show(50)

      file.outputJson("death_by_month",fin)
      file.outputcsv("death_by_month",fin)



    }


  def q4():Unit={


    test.show()
    val dd = test.withColumnRenamed("Last Update","Last_Update")

    dd.show()
    dd.write.mode("overwrite").saveAsTable("yeapijustdid")
    session.spark.sql("SELECT * FROM yeapijustdid").printSchema()
    session.spark.sql("SELECT * FROM yeapijustdid").show(5000)




  }



}
