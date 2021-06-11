package sunTask

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._

object StateFilterDSL {
  def main(args: Array[String]): Unit = {
			val conf = new SparkConf().setAppName("FilterStateLA").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")

					val spark = SparkSession.builder().master("local[*]").getOrCreate()
					import spark.implicits._

					//reading usdata
					val usdataDf = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("file:///E://data//usdata.csv")
					usdataDf.show(3)
					//Filter state = LA data
					val stateDf =  usdataDf.filter(col("state")==="LA")
					stateDf.show(3)

	}
}