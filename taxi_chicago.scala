import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Taxi{

  def main (args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.set("spark.testing.memory","2147480000")
    conf.setMaster("local[1]")

    val sparkSession = SparkSession
      .builder()
      .appName("Spark SQL Taxi")
      .config(conf)
      .getOrCreate()

    val path = "/Users/zongdongyu/Desktop/NUS/MSBA/Semster 1 - 2019/DSC5102 Business Analytics Capstone/Assignments/Spark/try.csv"
    val df = sparkSession.read.option("header","true").csv(path)
    df.show()

    df.select("Pickup Centroid Latitude").show()
  }
}

