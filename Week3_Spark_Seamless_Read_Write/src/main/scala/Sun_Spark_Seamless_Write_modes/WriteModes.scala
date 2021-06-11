package Sun_Spark_Seamless_Write_modes

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WriteModes {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSeamlessWriteMode").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._

    println("*****************************Reading usdata csv with header*****************************")
    val usdataDf = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("file:///E://data//usdata.csv")
    // inferSchema - schema will change according data type
    usdataDf.show(3)
    //usdataDf1.printSchema()

    /*println("*****************record format: parquet(default) and mode: error(default)*****************")
    usdataDf.write.save("file:///E://28output//Scala28Week3//class//mode//record_parquet_format")
    
    println("*****************record format: parquet(default) and mode: error)*****************")
    usdataDf.write.mode("error").save("file:///E://28output//Scala28Week3//class//mode//record_parquet_format")

    println("*****************record format: parquet(default) and mode: append)*****************")
    usdataDf.write.mode("append").save("file:///E://28output//Scala28Week3//class//mode//record_parquet_format")
    
     println("*****************record format: parquet(default) and mode: overwrite)*****************")
    usdataDf.write.mode("overwrite").save("file:///E://28output//Scala28Week3//class//mode//record_parquet_format")*/
    
     println("*****************record format: parquet(default) and mode: ignore)*****************")
    usdataDf.write.mode("ignore").save("file:///E://28output//Scala28Week3//class//mode//record_parquet_format")

  }
}