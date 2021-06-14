package sat

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object RevisonAllSpark {
  case class txnSchema(txnno: String, txndate: String, custno: String, amount: String, category: String, product: String, city: String, state: String, spendby: String)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Revision").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._

    val columnsList = List("category", "product", "txnno", "txndate", "amount", "city", "state", "spendby", "custno")

    println("***********1 List creation***********")
    println("adding 2 for each element add2")
    val listInt = List(1, 4, 6, 7)
    val resInt = listInt.map(x => x + 2)
    resInt.foreach(println)

    println("************2 fetching zeyo from list************")
    val listStr = List("zeyobron", "zeyo", "analytics")
    val resStr = listStr.filter(x => x.contains("zeyo"))
    resStr.foreach(println)

    println("******3 read fiel1.txt and fetch gymanstics*******")
    val file1 = sc.textFile("file:///E://data//revdata//file1.txt")
    val gymdata = file1.filter(x => x.contains("Gymnastics"))
    gymdata.take(5).foreach(println)

    println("***4 column based filter: product contains Gymnastics***")
    val mapsplit = gymdata.map(x => x.split(","))
    println("apply schema")
    val schemardd = mapsplit.map(x => txnSchema(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8)))
    val schemafilter = schemardd.filter(x => x.product.contains("Gymnastics"))
    schemafilter.take(3).foreach(println)

    println("************5 Row rdd************")
    val file2 = sc.textFile("file:///E://data//revdata//file2.txt")
    val rowmap = file2.map(x => x.split(","))
    val rowrdd = rowmap.map(x => Row(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8)))
    val rowfilter = rowrdd.filter(x => x(8).toString().contains("cash"))
    rowfilter.take(5).foreach(println)

    println("*******6 convert schema rdd to df**********")
    val schemadf = schemafilter.toDF().select(columnsList.map(col): _*)
    schemadf.show(5)

    println("*******convert row rdd to df**********")
    val schemaStruct = StructType(Array(
      StructField("txnno", StringType, true),
      StructField("txndate", StringType, true),
      StructField("custno", StringType, true),
      StructField("amount", StringType, true),
      StructField("category", StringType, true),
      StructField("product", StringType, true),
      StructField("city", StringType, true),
      StructField("state", StringType, true),
      StructField("spendby", StringType, true)))
    val rowdf = spark.createDataFrame(rowfilter, schemaStruct).select(columnsList.map(col): _*)
    rowdf.show(3)

    println("*******7 read file3 csv format***********")
    val csvdf = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///E://data//revdata//file3.txt").select(columnsList.map(col): _*)
    csvdf.show(3)
    csvdf.printSchema()

    println("********8 read json and parequet files*******")
    val jsondf = spark.read.format("json").load("file:///E://data//revdata//file4.json").select(columnsList.map(col): _*)
    jsondf.show(3)

    val parquetdf = spark.read.load("file:///E://data//revdata//file5.parquet").select(columnsList.map(col): _*)
    parquetdf.show(3)

    println("******9 XML file read**********")
    val xmldf = spark.read.format("com.databricks.spark.xml").option("rowTag", "txndata").load("file:///E://data//revdata//file6.xml").select(columnsList.map(col): _*)
    xmldf.show(3)

    println("**********10 Union all dfs**********")
    val uniondf = schemadf.union(rowdf).union(csvdf).union(jsondf).union(parquetdf).union(xmldf)
    uniondf.show(3)

    println("*******11 fetch year from date***********")
    val yeardf = uniondf.withColumn("txndate", expr("split(txndate,'-')[2]"))
    println("*******add status col, cash=1 credit =0***********")
    val checkdf = yeardf.withColumn("status", expr("case when spendby='cash' then 1 else 0 end"))
    println("*******get tanno <50000***********")
    val finaldf = checkdf.filter(col("txnno") < 50000)
    finaldf.show(3)

    println("*******12 write data as avro***********")
    finaldf.write.format("com.databricks.spark.avro").partitionBy("category").mode("overwrite").save("file:///E://28output//Scala28Week5//class//record_avro_format")
    println("***********data written***********")
  }
}