package sun

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ComplexDataProcessing {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ComplexDataProcessing").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._

    println("*********multiline json read**********")
    val jsonDf = spark.read.format("json").option("multiLine", "true").load("file:///E://data//zeyodata.json")
    jsonDf.show(3, false)
    jsonDf.printSchema()

    println("*********jsondf process**********")
    val flattenDf = jsonDf.select("No", "Year", "address.permanent_address", "address.temporary_address", "firstname", "lastname")
    flattenDf.show()
    flattenDf.printSchema()

    println("*********jsondf 2nd way process**********")
    val flattenDf2 = jsonDf.select("No", "Year", "address.*", "firstname", "lastname")
    flattenDf2.show()
  }
}