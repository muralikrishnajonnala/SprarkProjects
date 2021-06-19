package sat

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._

object Struct_Inside_Struct {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StructInsideStruct").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._

    println("*********multiline topping read**********")
    val toppingDf = spark.read.format("json").option("multiLine", "true").load("file:///E://data//topping.json")
    toppingDf.show()
    toppingDf.printSchema()

    println("*********toppingDf process**********")
    val flattenDf = toppingDf.select(
      col("batters.batter.id").alias("batters_id"),col("batters.batter.type").alias("batters_type"), 
      col("id"),col("name"), col("ppu"),
      col("topping.id").alias("topping_id"), col("topping.type").alias("topping_type"), 
      col("type"))
    flattenDf.show(false)
    flattenDf.printSchema()

    println("*********complex data genaration**********")
    val complexDf = flattenDf.select(
      struct(
        struct(
          col("batters_id").alias("id"),
          col("batters_type").alias("type")).alias("batter")).alias("batters"),
      col("id"),
      col("name"),
      col("ppu"),
      struct(
        col("topping_id").alias("id"),
        col("topping_type").alias("type")).alias("topping"),
      col("type"))
    complexDf.show(false)
    complexDf.printSchema()
  }
}