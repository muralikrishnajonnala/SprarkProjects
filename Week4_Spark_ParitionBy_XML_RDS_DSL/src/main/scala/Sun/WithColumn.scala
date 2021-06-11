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
					//operation4
					val categorydf = txnDf.withColumn("category", expr("split(category,' ')[0]"))
					categorydf.show(3)

					//operation5
					val cat1Df = txnDf.withColumn("cat1", expr("split(category,' ')[0]"))
					cat1Df.show(3)

					txnDf.withColumn("fixvalue", lit("1")).show(3)

	}

}