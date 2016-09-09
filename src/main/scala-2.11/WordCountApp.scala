/** WordCountApp.scala
  * Created by joshuaarnold on 9/9/16.
  */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object WordCountApp {

  def main(args: Array[String]) {
    val logFile = "spark_read_me.txt"
    val conf = new SparkConf().setAppName("Word Count Application")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}
