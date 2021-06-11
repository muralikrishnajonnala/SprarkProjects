package Sat_Seamless_Read_Write

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object SeamLessRead {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSeamlessRead").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._

    println("*****************************Reading usdata csv without header*****************************")
    val usdataDf = spark.read.format("csv").load("file:///E://data//usdata.csv")
    usdataDf.show(3)
    usdataDf.printSchema()

    println("*****************************Reading usdata csv with header*****************************")
    val usdataDf1 = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("file:///E://data//usdata.csv")
    // schema will change according data type
    //usdataDf1.show(3)
    usdataDf1.show(3)
    usdataDf1.printSchema()

    println("*****************************Reading Parquet *****************************")
    val parquetDF = spark.read.format("parquet").load("file:///E://data//part_par.parquet")
    // schema will change according data type
    parquetDF.show(3, false)
    parquetDF.printSchema()

    println("*****************************Reading Json *****************************")
    val jsonDf = spark.read.format("json").load("file:///E://data//devices.json")
    // schema will change according data type
    jsonDf.show(3, false)

  /*  println("*****************************Reading orc *****************************")
    val orcDF = spark.read.format("orc").option("header", "true").load("file:///E://data//part_orc.orc")
    orcDF.show(3, false)*/

    
  }
}