package sat

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Aggregations {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Revision").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._

    println("*******reading txn_header data***********")
    val csvdf = spark.read.format("csv").option("header", "true").load("file:///E://data//txns_withheader")

    val sumdf = csvdf.groupBy("category").agg(sum("amount").alias("sum_amt"))
    sumdf.show()

    val mindf = csvdf.groupBy("category").agg(min("amount").alias("min_amt"))
    mindf.show()

    val maxdf = csvdf.groupBy("category").agg(max("amount").alias("max_amt"))
    maxdf.show()

    val castToIntDf = csvdf.groupBy("category").agg(max("amount").alias("max_amt_int")).withColumn("max_amt_int", col("max_amt_int").cast(IntegerType))
    castToIntDf.show()
  }
}