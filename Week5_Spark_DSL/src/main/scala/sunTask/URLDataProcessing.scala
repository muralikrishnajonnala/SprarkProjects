package sunTask

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._

object URLDataProcessing {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("URL_Data_Processing").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._

    System.setProperty("http.agent", "Chrome")
    // reading url data
    val urlData = scala.io.Source.fromURL("https://reqres.in/api/users?page=3").mkString
    println("***********URL Data***************")
    println(urlData)

    val rdd = sc.parallelize(urlData :: Nil)
    val json_file = spark.read.option("multiLine", "false").json(rdd)
    json_file.show()
    json_file.printSchema()

    println("***********Processing complex data***************")
    val web_data_df = json_file.select(
      col("data"),
      col("page"),
      col("per_page"),
      col("support.text").alias("support_text"),
      col("support.url").alias("support_url"),
      col("total"),
      col("total_pages"))
    web_data_df.show()
    web_data_df.printSchema()
  }
}