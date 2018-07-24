---------------------------------small files--------------------------------------
package za.co.hadoop
import java.text.SimpleDateFormat
import org.apache.spark.sql.SparkSession
import java.io._
import org.apache.hadoop.fs.FileSystem
/**
  * Created by name on 6/25/2018.
  */
object SmallFiles {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("Combine Small Files").getOrCreate()
    val sourceDirectory = args(0)
    val outputDirectory = args(1)
    System.out.print("Source directory : " + sourceDirectory)
     val dataFrame = sparkSession.read.parquet(sourceDirectory + "/*.parquet")
    val sourceCount =  dataFrame.count()
    System.out.print("Number of records in original files : " + sourceCount + " ")
  dataFrame.repartition(1).write.option("compression","snappy").parquet(outputDirectory)
  }
}
---------------------------------oracle data-------------------------------------
package za.co.hadoop

import com.esotericsoftware.minlog.Log.Logger
import com.sun.glass.ui.Window.Level
import org.apache.spark.sql.SparkSession
import org.apache.log4j._

/**
  * Created by name on 7/17/2018.
  */
object InitialLoad {
  def main(args: Array[String]){
  val outputDirectory = args(0)
  // Set the log level to only print errors
  //Logger.getLogger("org").setLevel(Level.ERROR)
   val spark = SparkSession.builder().appName("CDO-ODBC").getOrCreate()
   val url = "jdbc:oracle:thin:user/pass@host:port/SIEBELDB"
   val table = "S_ORG_EXT"
    val sDF = spark.read.format("jdbc")
      .options(Map("driver"->"oracle.jdbc.driver.OracleDriver","url" -> url,"dbtable" -> table))
      .load()

    //sDF.write.format("parquet").save("complexEntityResult.parquet")
    sDF.repartition(1).write.option("compression","snappy").parquet(outputDirectory)
    sDF.show()

  }

}
--------------------------------------mysql----------------------------------------------------
package za.co.hadoop
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by name on 7/23/2018.
  */
object FetchFromSql {


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("GET DATA FROM MySQL").getOrCreate()
    val URL =  "jdbc:mysql://host:3306/bakhona"
    val user = "ngwenyba_adm"
    val pass = "";
    val TABLE_NAME = args(0)
    val OUTPUT_DIR = args(1)

    print("The selected table from MYSQL DATABASE is : " + TABLE_NAME)

    val df = spark.read.format("jdbc")
      .options(Map("driver"->"com.mysql.jdbc.Driver","url" -> URL, "user" -> user, "password" -> pass ,"dbtable" -> TABLE_NAME))
      .load()
    df.printSchema()
    df.show()
    df.repartition(1).write.option("compression" , "snappy").parquet(OUTPUT_DIR)
  }
}

