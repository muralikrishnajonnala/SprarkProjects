package sun

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexDataGenarationRepapi {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ComplexData_Processing_Genaration").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    println("*********multiline json read**********")
    val jsonDf = spark.read.format("json").option("multiLine", "true").load("file:///E://data//reqapi.json")
    jsonDf.show(3, false)
    jsonDf.printSchema()

    println("*********flatten data**********")
    val flattenDf = jsonDf.select(col("page"),col("per_page"),col("total"),col("total_pages"),
         col("data.id").alias("data_id"), col("data.email").alias("data_email"), 
         col("data.first_name").alias("data_first_name"), col("data.last_name").alias("data_last_name"),col("data.avatar").alias("data_avatar"),
         col("support.url").alias("support_url"),col("support.text").alias("support_text"))
         
    flattenDf.show()
    flattenDf.printSchema()

    println("*********complex data generation**********")
    val complexDf = flattenDf.select(
                                      col("page"),
                                      col("per_page"),
                                      col("total"),
                                      col("total_pages"),
                                   struct(
                                          col("data_id").alias("id"),
                                          col("data_email").alias("email"),
                                          col("data_first_name").alias("first_name"),
                                          col("data_last_name").alias("last_name"),
                                          col("data_avatar").alias("avatar")
                                          ).alias("data"),
                                   struct(
                                          col("support_url").alias("url"),
                                          col("support_text").alias("text")
                                          ).alias("support")
                                      )
  
  complexDf.show()
  complexDf.printSchema()
  }
}