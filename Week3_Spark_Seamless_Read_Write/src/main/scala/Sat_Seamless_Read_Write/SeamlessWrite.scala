package Sat_Seamless_Read_Write

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object SeamlessWrite {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SparkSeamlessWrite").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.sqlContext.implicits._

    println("*****************************Reading Json *****************************")
    val jsonDf = spark.read.format("json").load("file:///E://data//devices.json")
    // schema will change according data type
    jsonDf.show(3, false)

    println("*****************write format: parquet(default)*****************")
    jsonDf.write.save("file:///E://28output//Scala28Week3//class//record_parquet_format")

    println("*****************write format:json *****************")
    jsonDf.write.format("json").save("file:///E://28output//Scala28Week3//class//record_json_format")

    println("*****************write format:avro*****************")
    jsonDf.write.format("com.databricks.spark.avro").save("file:///E://28output//Scala28Week3//class//record_avro_format")

   /* println("*****************record format:orc and mode:overwrite*****************")
    jsonDf.write.format("orc").save("file:///E://28output//Scala28Week3//class//record_orc_format")*/

  }

}