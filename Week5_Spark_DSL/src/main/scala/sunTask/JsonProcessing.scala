package sunTask

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions._

object JsonProcessing {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("From_To_Json").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._

    println("*********read devices json with delimiter ~**********")
    val deviceDf = spark.read.format("csv").option("delimiter", "~").load("file:///E://data//devices.json")
    deviceDf.show(3, false)
    val weblogschema_Struct = StructType(Array(
					StructField("device_id", StringType, true),
					StructField("device_name", StringType, true),
					StructField("humidity", StringType, true),
					StructField("lat", StringType, true),
					StructField("long", StringType, true),    
					StructField("scale", StringType, true),
					StructField("temp", StringType, true),
					StructField("timestamp", StringType, true),
					StructField("zipcode", StringType, true)));  
    
    println("*********processing using from_json**********")
    val fromJsonDF = deviceDf.withColumn("_c0", from_json(col("_c0"), weblogschema_Struct)).select(col("_c0.*"))
    fromJsonDF.show(3)
    
    println("*********converting DF to json using to_json*****")
    val toJsonDf = fromJsonDF.select(to_json(struct(col("*"))).alias("_c0"))
    toJsonDf.show(3,false)
  }
}