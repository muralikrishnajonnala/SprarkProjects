package sunTask

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SchemaFromJsonDataFile {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SchemaFromJsonData").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._

    println("***************Reading Json******************")
    val jsonDf = spark.read.format("json").load("file:///E://data//devices.json")
    jsonDf.show(3)
    println("***************Printing Schema****************")
    jsonDf.schema.foreach(println)
  }
}