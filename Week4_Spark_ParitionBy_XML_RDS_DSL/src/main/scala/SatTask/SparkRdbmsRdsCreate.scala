package SatTask

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
// added extra dependency for mysql connector
object SparkRdbmsRdsCreate {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RDBMSRead").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.sqlContext.implicits._

    val txnDF = spark.read.format("csv").option("header", "true").load("file:///E://data//txns_withheader")

    println("**********Write Start***********")

    // writing data into amazon sql table-murali_tab
    val df = txnDF.write.format("jdbc")
      .option("url", "jdbc:mysql://mysql56.cki8jgd5zszv.ap-south-1.rds.amazonaws.com:3306/batch28")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", "murali_tab")
      .option("user", "root")
      .option("password", "Aditya908")
      .mode("append")
      .save()

    println("**********Write Complete*************")
  }

}