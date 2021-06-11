package Sun_Spark_ExternalJar

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object SeamLessReadAndWrite {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSeamlessReadAndWrite").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._
    
    //local

   /* println("********************Reading usdata csv with header*****************")
    val usdataDf = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("file:///E://data//usdata.csv")
    usdataDf.show(3)

    println("*****************record format:avro and mode:overwrite*****************")
    usdataDf.write.format("com.databricks.spark.avro").save("file:///E://28output//Scala28Week3//class//sun//record_avro_format")*/

    //cloudera
    println("*******************Reading usdata csv with header*********************")
    val usdataDf = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("file:///home/cloudera/data/usdata.csv")
    usdataDf.show(3)
    println("*****************record format:avro and mode: append*****************")
    usdataDf.write.format("com.databricks.spark.avro").mode("append").save("hdfs:/user/cloudera/data/avrowrite_spark")

  }
}