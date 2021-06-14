package Sat

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
// added extra dependency for mysql connector
object SparkRDBMSRead {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDBMSRead").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.sqlContext.implicits._

    println("**********Read Start**********")
    val sqlDf = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://mysql56.cki8jgd5zszv.ap-south-1.rds.amazonaws.com:3306/batch28")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", "murali_tab")
      .option("user", "root")
      .option("password", "Aditya908")
      .load()

    sqlDf.show()
    //sqlDf.printSchema()

    println("**********Read Complete*************")
  }
}