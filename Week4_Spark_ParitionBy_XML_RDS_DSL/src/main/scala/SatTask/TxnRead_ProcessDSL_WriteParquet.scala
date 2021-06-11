package SatTask

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._

object TxnRead_ProcessDSL_WriteParquet {
   def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("DSLReadPaquetWrite").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.sqlContext.implicits._

    val txnDF = spark.read.format("csv").option("header", "true").load("file:///E://data//txns_withheader")
    txnDF.show(3)
    
    val fil_data = txnDF.filter(col("category")==="Gymnastics")
    fil_data.show(3)
    
    println("**********write format: parquet(default)********")
    fil_data.write.save("file:///E://28output//Scala28Week4//task//record_parquet_format")
    println("**********write complete***********")
}
}