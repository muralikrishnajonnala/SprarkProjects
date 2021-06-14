package sunTask

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._

object WithColRDSWrite {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WithColumn").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._

    //reading txn data
    val txnDf = spark.read.option("header", true).format("csv").load("file:///E://data//txns_withheader")
    txnDf.show(5)
    //in category, no Gymnastics, Team Sports
    val withcolDf = txnDf.filter(!col("category").isin("Gymnastics", "Team Sports"))
      .withColumn("cat2", expr("case when category in('Exercise & Fitness','Outdoor Recreation') then 'Outdoor' else 'indore' end"))
    withcolDf.show()

    println("**********RDS Write Start*************")

    // writing data into amazon sql table:murali_task_tab
    val df = withcolDf.write.format("jdbc")
      .option("url", "jdbc:mysql://mysql56.cki8jgd5zszv.ap-south-1.rds.amazonaws.com:3306/batch28")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", "murali_task_tab")
      .option("user", "root")
      .option("password", "Aditya908")
      .mode("overwrite")
      .save()
    println("**********RDS Write Complete*************")

  }

}