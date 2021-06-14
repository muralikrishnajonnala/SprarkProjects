package sun

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexDataGenarationPicture {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ComplexData_Processing_Genaration").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    println("*********multiline json read**********")
    val jsonDf = spark.read.format("json").option("multiLine", "true").load("file:///E://data//picture.json")
    jsonDf.show(3, false)
    jsonDf.printSchema()

    println("*********flatten data**********")
    val flattenDf = jsonDf.select(col("id"), col("image.height").alias("image_height"), col("image.url").alias("image_url"), col("image.width").alias("image_width"), 
                    col( "name"), 
                    col("thumbnail.height").alias("thumbnail_height"),col("thumbnail.url").alias("thumbnail_url"),col("thumbnail.width").alias("thumbnail_width"),
                    col("type"))
    flattenDf.show(false)
    flattenDf.printSchema()

    println("*********complex data generation**********")
    val complexDf = flattenDf.select(
                                      col("id"),
                                   struct(
                                          col("image_height").alias("height"),
                                          col("image_height").alias("url"),
                                          col("image_width").alias("width")
                                          ).alias("image"),
                                   col("name"),
                                   struct(
                                          col("thumbnail_height").alias("height"),
                                          col("thumbnail_url").alias("url"),
                                          col("thumbnail_width").alias("width")
                                          ).alias("thumbnail"),
                                     col("type")
                                      )
  
  complexDf.show(false)
  complexDf.printSchema()
  }
}