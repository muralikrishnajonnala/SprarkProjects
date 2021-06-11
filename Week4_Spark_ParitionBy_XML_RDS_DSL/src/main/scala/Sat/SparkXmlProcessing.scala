package Sat

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object SparkXmlProcessing {
  //com.databricks.spark.xml - data will use for read xml only
  // to write additional jars required
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("XMLProcessing").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.sqlContext.implicits._

    println("**********Reading note xml data start***********")
    val noteXmlDf = spark.read.format("com.databricks.spark.xml").option("rowTag", "note").load("file:///E://data//note.xml")
    noteXmlDf.show(3, false)
    noteXmlDf.printSchema()
    println("**********Reading note xml data end***************")

    println("**********Reading book xml data start***********")
    val bookXmlDF = spark.read.format("com.databricks.spark.xml").option("rowTag", "book").load("file:///E://data//complexjson//book.xml")
    bookXmlDF.show(3, false)
    bookXmlDF.printSchema()
    println("**********Reading book xml data end***************")

    println("**********Reading transactions xml data start***********")
    val transactionsXmlDF = spark.read.format("com.databricks.spark.xml").option("rowTag", "POSLog").load("file:///E://data//complexjson//transactions.xml")
    transactionsXmlDF.show(3, false)
    transactionsXmlDF.printSchema()
    println("**********Reading transactions xml data end***************")
  }

}