package P2

import P2.Main.session
import P2.P2tempviews.df6
import org.apache.spark.sql.functions.col



object Queries{


    def createTablesE(): Unit = {


//      val dfcovidFromFile = session.spark.read.option("delimiter", ",").option("inferSchema","true").option("header", "true")
//        .csv(s"hdfs://localhost:9000/user/hive/warehouse/covid_19_data.csv")

      df6.createOrReplaceTempView("cd")

      val dfCovid_data=(df6.withColumnRenamed("Country/Region", "region_country")
        .withColumnRenamed("Province/State", "province_state")
        .withColumnRenamed("Last Update", "Last_Update")
        .withColumn("Confirmed", col("Confirmed").cast("int"))
        .withColumn("Deaths", col("Deaths").cast("int"))
        .withColumn("Recovered", col("Recovered").cast("int"))
        )

      dfCovid_data.show()
      dfCovid_data.createOrReplaceTempView("covid_view")
      dfCovid_data.repartition(10).write.mode("overwrite").save("hdfs://localhost:9000/user/hive/warehouse/new_covid_19_data.parquet")
      val parquetfiledf = session.spark.read.parquet("hdfs://localhost:9000/user/hive/warehouse/new_covid_19_data.parquet")
      parquetfiledf.createOrReplaceTempView("parquet_view")

      /*Table for Cities*/
      session.spark.sql("CREATE TABLE IF NOT EXISTS cities AS SELECT SNo, province_state FROM parquet_view")



      /*Table for Countries*/
      session.spark.sql("CREATE TABLE IF NOT EXISTS countries AS SELECT SNo, region_country FROM parquet_view")



      /*Table for covid_data*/
      session.spark.sql("CREATE TABLE IF NOT EXISTS covid_data AS SELECT SNo, ObservationDate, Last_Update, " +
        "Confirmed, Deaths, Recovered FROM parquet_view")


    }


    def query1(v1:Boolean,v2:Boolean): Unit={

      /*    - How many people were recovered worldwide by the last quarter of 2020

            Enter your query here
       */
      val fin = session.spark.sql("SELECT ObservationDate, COUNT(recovered) AS Total_Recovered FROM covid_data where ObservationDate BETWEEN '10/01/2020' AND '12/31/2020' " +
        "GROUP BY ObservationDate ORDER BY ObservationDate ASC").toDF()
      if(v1){
        fin.show()
      }
      if(v2){
        file.outputJson("RecoveryWorldwideLastQuarter",fin)
        file.outputcsv("RecoveryWorldwideLastQuarter",fin)
      }




    }

  def query2(v1:Boolean,v2:Boolean): Unit={
    /*    - What are the top 10 cities with number of deaths in the US?

            Enter your query here
       */
    val fin = session.spark.sql("SELECT province_state, COUNT(deaths) AS Total_Deaths FROM cities c JOIN covid_data cov ON (c.sno = cov.sno) JOIN countries co " +
      "ON (c.sno = co.sno) WHERE co.region_country = 'US' GROUP BY province_state ORDER BY Total_Deaths DESC LIMIT 10").toDF()

    if(v1){
      fin.show()
    }
    if(v2){
      file.outputJson("top10CitiesDeaths",fin)
      file.outputcsv("top10CitiesDeaths",fin)
    }

  }
  def query3(v1:Boolean,v2:Boolean): Unit={

    /*    - What are the countries with most recovered covid cases during the pandemic?

            Enter your query here
       */
    val fin = session.spark.sql("SELECT region_country, COUNT(recovered) AS Total_Recovered FROM countries co JOIN covid_data cov ON(co.sno = cov.sno) " +
      "GROUP BY region_country HAVING COUNT(confirmed) > 200 ORDER BY Total_Recovered DESC").toDF()

    if(v1){
      fin.show()
    }
    if(v2){
      file.outputJson("countriesRecovered",fin)
      file.outputcsv("countriesRecovered",fin)
    }


  }

}