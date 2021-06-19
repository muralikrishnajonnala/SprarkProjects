package sat

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._

object JsonArrayProcessing {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JsonArrayProcess").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._

    println("*********multiline topping read**********")
    val arrayjsonDf = spark.read.format("json").option("multiLine", "true").load("file:///E://data//arrayjson.json")
    arrayjsonDf.show()
    arrayjsonDf.printSchema()

    /* println("*********arrayjsonDf process**********")
    val flattendf = arrayjsonDf.select(
      "first_name",
      "second_name",
      "Students",
      "address.Permanent_address",
      "address.temporary_address")

    val explodf = flattendf.withColumn("Students", explode(col("Students")))
    explodf.printSchema()
    explodf.show()*/

    println("*********arrayjsonDf process select with withColumn**********")
    val explodeDf = arrayjsonDf.select(
      "first_name",
      "second_name",
      "Students",
      "address.Permanent_address",
      "address.temporary_address").withColumn("Students", explode(col("Students")))
    explodeDf.printSchema()
    explodeDf.show()

    println("*********complex data genaration-Array creation**********")
    val complexDf = explodeDf.groupBy("first_name", "second_name", "Permanent_address", "temporary_address")
      .agg(collect_list("Students").alias("Students"))
    complexDf.show()
    println("*********complex data genaration-Struct creation**********")
    val finalDf = complexDf.select(
      col("Students"),
      struct(
        col("Permanent_address"),
        col("temporary_address")).alias("adress"),
      col("first_name"),
      col("second_name"))

    finalDf.show(false)
    finalDf.printSchema()
  }
}