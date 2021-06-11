package Sun

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._

object DSL_Operations {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DSLOperations").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._

    //reading txn data
    val txnDf = spark.read.option("header", true).format("csv").load("file:///E://data//txns_withheader")
    //operation1
    val col_list = List("txnno", "txndate", "amount", "category", "product", "spendby")
    val col_listDF = txnDf.select(col_list.map(col): _*)
    col_listDF.show(3)
    //operation2
    val txnSpendbyFileter = txnDf.filter(col("txnno") > 50000 && col("spendby") === "cash")
    txnSpendbyFileter.show(3)

    //operation3
    val txnSpendbyWeightLIftFileter = txnSpendbyFileter.filter(col("product").like("Weightlifting%"))
    txnSpendbyWeightLIftFileter.show(3)

  }

}