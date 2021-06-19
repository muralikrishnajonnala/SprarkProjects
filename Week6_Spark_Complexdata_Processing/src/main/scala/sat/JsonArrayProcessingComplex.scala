package sat

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._

object JsonArrayProcessingComplex {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JsonArrayProcess").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._

    println("*********multiline json read**********")
    val arrayjsonDf = spark.read.format("json").option("multiLine", "true").load("file:///E://data//array1.json")
    arrayjsonDf.show()
    arrayjsonDf.printSchema()

    println("*********flatten json**********")
    val flattenDf = arrayjsonDf.select(
      col("Students"),
      col("address.Permanent_address"),
      col("address.temporary_address"),
      col("first_name"),
      col("second_name"))
    flattenDf.show()
    flattenDf.printSchema()
    
    println("*********process all array fields using explode**********")
    val explodeDf = flattenDf.withColumn("Students", explode(col("Students")))
    explodeDf.show()
    explodeDf.printSchema()

    println("*********process all strutsfields**********")
    val finalDf = explodeDf.select(
      col("Students.user.address.Permanent_address").alias("s_u_a_paddress"),
      col("Students.user.address.temporary_address").alias("s_u_a_taddress"),
      col("Students.user.gender"),
      col("Students.user.name.first").alias("s_e_u_a_first"),
      col("Students.user.name.last").alias("s_e_u_a_last"),
      col("Students.user.name.title").alias("s_e_u_a_title"),
      col("address.Permanent_address").alias("a_Permanent_address"),
      col("address.temporary_address").alias("a_temporary_address"),
      col("first_name"),
      col("second_name"))
    finalDf.printSchema()
    finalDf.show()

  }
}