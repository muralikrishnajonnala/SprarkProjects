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
    //operation1: select
    val col_list = List("txnno", "txndate", "amount", "category", "product", "spendby")
    //Select from list
    val col_listDF = txnDf.select(col_list.map(col): _*)
    col_listDF.show(3)
    //calling selected cols another way
    val sel_col = txnDf.select("txnno","category","product")
    sel_col.show(3)

    //operation2: filter
    val categoryFileter = txnDf.filter(col("category") === "Gymnastics")
    categoryFileter.show(3)
    val categoryFileter2 = txnDf.filter($"category" === "Gymnastics")
    categoryFileter2.show(3)
    
    //multiple conditions: filter by category === Gymnastics and spenby =!= to cash
    val txnSpendbyFilter = txnDf.filter(col("category") === "Gymnastics" && col("spendby") =!= "cash")
    txnSpendbyFilter.show(3)
    
    //multiple conditions: filter by category === Gymnastics or spenby =!= to cash
    val txnSpendbyFilterOr = txnDf.filter(col("category") === "Gymnastics" || col("spendby") =!= "cash")
    txnSpendbyFilterOr.show(3)

    //operation3:isin
    //isin : category === Gymnastics contains Exercise & Fitness,Gymnastics, Team Sports
    val isin = txnDf.filter(col("category").isin("Exercise & Fitness","Gymnastics","Team Sports"))
    isin.show(5)
    
    //not isin : category === Gymnastics contains Exercise & Fitness,Gymnastics, Team Sports
    val notisin = txnDf.filter(!col("category").isin("Exercise & Fitness","Gymnastics","Team Sports"))
    notisin.show(5)
    
    //operation4: like on product column
    val likeDf = txnSpendbyFilter.filter(col("product").like("Gymnastics%"))
    likeDf.show(3,false)
    
    //not like on product column
    val notlikeDf = txnSpendbyFilter.filter(!col("product").like("Gymnastics%"))
    notlikeDf.show(3,false)
  }
}