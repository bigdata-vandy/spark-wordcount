/** WordCountApp.scala
  * Created by joshuaarnold on 9/9/16.
  */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object WordCountApp {

  def main(args: Array[String]) {
    val textFile = "spark_read_me.txt"
    val conf = new SparkConf()
      .setAppName("Word Count Application")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val textData = sc.textFile(textFile, 2).cache()

    val wordFrequencies = textData flatMap (_ split ("\\s+")
      map (word => (word, 1))) reduceByKey (_ + _)

    // Prints results
    (wordFrequencies.takeOrdered(50)(Ordering[Int].reverse.on(_._2)))
        .foreach(println)

  }
}
