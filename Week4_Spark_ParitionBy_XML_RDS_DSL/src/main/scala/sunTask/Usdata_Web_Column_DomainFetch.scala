package sunTask

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._

object Usdata_Web_Column_DomainFetch {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DomainData").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._

    //reading usdata
    val usdataDf = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("file:///E://data//usdata.csv")
    usdataDf.show(3)
    println("*******from web col fetch domain name*******")
    val domainDf = usdataDf.withColumn("web", expr("split(web,'\\\\.')[1]"))
    domainDf.show(3)

  }
}