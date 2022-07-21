
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.sql.DataFrame
import java.security.MessageDigest
import org.apache.spark.sql.functions.{col, desc, asc, isnull, month, not, to_date, udf, when, year}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.commons.io.FileUtils
import scala.util.Try
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, isnull, lag, lit, month, not, to_date, when}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.Try

object CovidP2Thuva {

  def outputcsv(name : String,newData:DataFrame): Unit =  {

    val outputfile = "C:\\outputcsv"
    var filename = name + ".csv"
    var outputFileName = outputfile + "/temp_" + filename
    var mergedFileName = outputfile + "/" + filename//merged_
    var mergeFindGlob  = outputFileName
    var fileDel = outputfile + "/." + filename + ".crc"

    newData.coalesce(1).write //Added
      .format("csv")
      .options(Map("header" -> "true", "delimiter" -> ",")) //Added
      .mode(SaveMode.Overwrite) //Added
      .save(outputFileName)
    merge(mergeFindGlob, mergedFileName,fileDel)
    newData.unpersist()
  }

  def merge(srcPath: String, dstPath: String,delPath: String): Unit =  {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), true, hadoopConfig)
    // the "true" setting deletes the source files once they are merged into the new output
    hdfs.delete(new Path(delPath),true)
  }


  def copyMerge(srcFS: FileSystem, srcDir: Path, dstFS: FileSystem, dstFile: Path, deleteSource: Boolean, conf: Configuration): Boolean = {
    if (dstFS.exists(dstFile)) {
      //throw new IOException(s"Target $dstFile already exists")
      dstFS.delete(dstFile,true)
    }
    // Source path is expected to be a directory:
    if (srcFS.getFileStatus(srcDir).isDirectory) {

      val outputFile = dstFS.create(dstFile)
      try {
        srcFS
          .listStatus(srcDir)
          .sortBy(_.getPath.getName)
          .collect {
            case status if status.isFile =>
              val inputFile = srcFS.open(status.getPath)
              try { IOUtils.copyBytes(inputFile, outputFile, conf, false) }
              finally { inputFile.close() }
          }
      } finally { outputFile.close() }

      if (deleteSource) srcFS.delete(srcDir, true) else true
    }
    else false
  }




  def StatePopVSDeath(): Unit = {
    var df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("input/time_series_covid_19_deaths_US.csv")
    df = df.withColumnRenamed("Province_state", "USState")
      .withColumnRenamed("5/2/21", "Deaths")
      .withColumn("Population", col("Population").cast("int"))
      .withColumn("Deaths", col("Deaths").cast("int"))

    df = df.select("USState", "Population", "Deaths").groupBy("USState").sum("Population", "Deaths").orderBy("USState")

    val ListOrder = df.orderBy(desc("sum(Population)"))
    outputcsv("StatePopulationVShDeath.csv",ListOrder )
    df.show(false)
  }



  System.setProperty("hadoop.home.dir", "C:\\Hadoop") //spark session for windows
  val spark = SparkSession
    .builder
    .appName("hello hive")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()
  println("Sucessfully Created Spark Session")
  spark.sparkContext.setLogLevel("ERROR")

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)




  def main(args: Array[String]) {
    StatePopVSDeath()

  }

  }


