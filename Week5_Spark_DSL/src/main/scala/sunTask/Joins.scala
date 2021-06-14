package sunTask

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Joins {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Joins").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._

    //reading j1 file
    val j1Df = spark.read.option("header", true).format("csv").load("file:///E://data//j1.csv")
    j1Df.show()
    //reading j2 file
    val j2Df = spark.read.option("header", true).format("csv").load("file:///E://data//j2.csv")
    j2Df.show()
    println("**************InnerJoin*******************")
    val innerJoinDf = j1Df.join(j2Df, j1Df("txnno") === j2Df("txn_number"), "inner")
    innerJoinDf.show()

    println("*********InnerJoin without ambiguous column ****************")
    val innerJoinDf_drop = j1Df.join(j2Df, j1Df("txnno") === j2Df("txn_number"), "inner").drop(j2Df.col("txn_number"))
    innerJoinDf_drop.show()

    /*println("*********Inner without ambiguous column  using sequence**********")
    println("seq can use both dfs have same column")
    val innerJoinDf_seq = j1Df.join(j2Df,Seq("txn_number"), "inner")
    innerJoinDf_seq.show()*/
    
    println("**************LeftJoin without ambiguous column ********************")
    val leftJoinDf_drop = j1Df.join(j2Df, j1Df("txnno") === j2Df("txn_number"), "left").drop(j2Df.col("txn_number"))
    leftJoinDf_drop.show()
    
    println("**************RightJoin without ambiguous column ********************")
    val rightJoinDf_drop = j1Df.join(j2Df, j1Df("txnno") === j2Df("txn_number"), "right").drop(j2Df.col("txn_number"))
    rightJoinDf_drop.show()

    println("**************OuterJoin without ambiguous column ********************")
    val outerJoinDf_drop = j1Df.join(j2Df, j1Df("txnno") === j2Df("txn_number"), "outer").drop(j2Df.col("txn_number"))
    outerJoinDf_drop.show()

  }
}