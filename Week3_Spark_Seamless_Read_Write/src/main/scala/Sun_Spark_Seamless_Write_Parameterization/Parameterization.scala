package Sun_Spark_Seamless_Write_Parameterization

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Parameterization {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSeamlessParameterization").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._

    val src = args(0) // file:///home/cloudera/data/usdata.csv
    val dest = args(1) // hdfs:/user/cloudera/batch28/week3/task/parameterization/formats/avro
    val mode = args(2) // error
    val writeformat = args(3) //com.databricks.spark.avro
    val soruceformat = args(4) //csv

    //cloudera
    println("*****************************Reading usdata csv with header*****************************")
    val usdataDf = spark.read.format(soruceformat).load(src)
    usdataDf.show(3)
    println("*****************record format:avro and mode: append*****************")
    usdataDf.write.format(writeformat).mode(mode).save(dest)

  }
}