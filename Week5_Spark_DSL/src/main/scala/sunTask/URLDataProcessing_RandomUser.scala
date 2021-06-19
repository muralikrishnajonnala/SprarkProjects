package sunTask

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._

object URLDataProcessing_RandomUser {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("URL_Data_Processing").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._

    System.setProperty("http.agent", "Chrome")
    // reading url data
    val urlData = scala.io.Source.fromURL("https://randomuser.me/api/0.8/?results=10").mkString
    println("***********URL Data***************")
    println(urlData)

    val url_rdd = sc.parallelize(urlData :: Nil)
    val json_file_Df = spark.read.option("multiLine", "false").json(url_rdd)
    json_file_Df.show()
    json_file_Df.printSchema()

    println("***********Processing complex data***************")
    val web_users_df = json_file_Df.withColumn("results", explode(col("results")))
      .select("nationality", "results.user.cell", "results.user.dob", "results.user.email", "results.user.gender", "results.user.location.city",
        "results.user.location.state", "results.user.location.street", "results.user.location.zip", "results.user.md5", "results.user.name.first",
        "results.user.name.last", "results.user.name.title", "results.user.password", "results.user.phone", "results.user.picture.large",
        "results.user.picture.medium", "results.user.picture.thumbnail", "results.user.registered", "results.user.salt", "results.user.sha1",
        "results.user.sha256", "results.user.username", "seed","version")

    web_users_df.show()
    web_users_df.printSchema()
    println("Count: "+web_users_df.count())
  }
}