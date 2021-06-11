package sunTask

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._

object WithColumnNotIn {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WithColumn").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._

    //reading txn data from edgenaode
    val txnDf = spark.read.option("header", true).format("csv").load("file:///home/cloudera/data/txns_withheader")
    //txnDf.show(5)
    //in category, no Gymnastics, Team Sports
    val withcolDf = txnDf.filter(!col("category").isin("Gymnastics", "Team Sports"))
      .withColumn("cat2", expr("case when category in('Exercise & Fitness','Outdoor Recreation') then 'Outdoor' else 'indore' end"))
    //withcolDf.show()

    // writing data in HDFS in avro format
    println("**********in HDFS write format:avro***********")
    withcolDf.write.format("com.databricks.spark.avro").mode("overwrite").save("hdfs:/user/cloudera/batch28/week4/task/avrowrite_spark")

    println("**********Write end*************")

  }

}