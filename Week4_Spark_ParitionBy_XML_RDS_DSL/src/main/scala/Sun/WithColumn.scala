package Sun

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._

object WithColumn {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WithColumn").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._

    //reading txn data
    val txnDf = spark.read.option("header", true).format("csv").load("file:///E://data//txns_withheader")
    txnDf.show(3)

    // read txndata, fetch txnno, from txndate fetch year as column year and amount
    //val trandf=txnDf.select("txnno","split(txndate,'-')[2] as year","amount")

    //operation5:
    println("*******selectExpt: with columns, expression can pass as column********")
    val transdf = txnDf.selectExpr("txnno", "split(txndate,'-')[2] as year", "amount")
    transdf.show(5)

    //operation6:
    println("****performing transformation on particular column****")
    val existingColDf = txnDf.withColumn("txndate", expr("split(txndate,'-')[2] as year"))
    existingColDf.show(3)

    println("****year column not exiting, so it will create new column at end****")
    val newColDf = txnDf.withColumn("year", expr("split(txndate,'-')[2] as year"))
    newColDf.show(3)

    println("**Performing transformation on particular column and renaming the existing column**")
    val renameColDf = txnDf.withColumn("txndate", expr("split(txndate,'-')[2] as year")).withColumnRenamed("txndate", "year")
    renameColDf.show(3)

    println("**on category column: split and fetch 0 index values**")
    val categorydf = txnDf.withColumn("category", expr("split(category,' ')[0]"))
    categorydf.show(3)
    
    println("**create new col cat1: split on category,fetch 0 index values**")
    val cat1Df = txnDf.withColumn("cat1", expr("split(category,' ')[0]"))
    cat1Df.show(3)

    println("**hard code a extra column with fixed value at the end of reach row**")
    txnDf.withColumn("fixvalue", lit("1")).show(3)
    
    println("**performing case on spendby applying result on fixvalue column**")
    txnDf.withColumn("fixvalue", expr("case when spendby='credit' then 1 else 0 end")).show(5)
  }
}