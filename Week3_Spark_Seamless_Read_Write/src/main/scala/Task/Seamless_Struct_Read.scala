package Task

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object Seamless_Struct_Read {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.sqlContext.implicits._
    spark.sparkContext.setLogLevel("Error")

    val struct =
      StructType(
        StructField("txnno", IntegerType, true) ::
          StructField("txndate", StringType, false) ::
          StructField("custno", StringType, true) ::
          StructField("amount", DoubleType, true) ::
          StructField("category", StringType, false) ::
          StructField("product", StringType, true) ::
          StructField("city", StringType, true) ::
          StructField("state", StringType, true) ::
          StructField("spendby", StringType, false) :: Nil)

    val seamsdf = spark.read.schema(struct).format("csv").load("file:///E://data//txns")
    println("*****************seamless df*****************")
    seamsdf.show(3)
    seamsdf.printSchema()

  }
}