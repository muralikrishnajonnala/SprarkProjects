package sun

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexDataGenaration {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ComplexDataProcessing").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    println("*********multiline json read**********")
    val jsonDf = spark.read.format("json").option("multiLine", "true").load("file:///E://data//zeyodata.json")
    jsonDf.show(3, false)
    jsonDf.printSchema()

    println("*********jsondf process**********")
    val flattenDf = jsonDf.select("No", "Year", "address.permanent_address", "address.temporary_address", "firstname", "lastname")
    flattenDf.show()
    flattenDf.printSchema()

    println("*********complex data genaration**********")
    val complexDf = flattenDf.select(
                                 struct(
                                      col("No"),
                                      col("Year"),
                                   struct(
                                          col("permanent_address"),
                                          col("temporary_address")
                                          ).alias("address"),
                                   struct(
                                          col("firstname"),
                                          col("lastname")
                                          ).alias("name")
                                         ).alias("record")
                                      )
  
  complexDf.show()
  complexDf.printSchema()
  }
}