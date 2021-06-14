package Sat

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object SparkPartition {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Partitions").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.sqlContext.implicits._

    println("**********Reading txns data start***********")
    val txnDf = spark.read.option("header", true).format("csv").load("file:///E://data//txns_withheader")
    txnDf.show(3, false)
    println("**********Reading txns data end*************")

    // single partition
    /*println("**********write format: parquet(default) Partition**********")
    txnDf.write.partitionBy("category").save("file:///E://28output//Scala28Week4//class//Partition")
    println("*************writing txns Partition end ***************")*/

    //partition-sub partition
    println("**********write format: parquet(default) SubPartiiton***********")
    txnDf.write.partitionBy("category", "spendby").mode("overwrite").save("file:///E://28output//Scala28Week4//class//SubPartition")
    println("*************writing txns SubPartition end ***************")

  }

}