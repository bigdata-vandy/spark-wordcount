/** WordCountApp.scala
  * Created by arnold-jr on 9/9/16.
  */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object WordCountApp {

  def main(args: Array[String]) {
    if (args.length != 2) {
     System.err.println("Requires HDFS input(s) as argument")
     System.exit(1)
    }
    val textFileName: String = args(0)
    val outputDir: String = args(1)

    val conf = new SparkConf()
      .setAppName("Word Count Application")
    val sc = new SparkContext(conf)
    val textData = sc.textFile(textFileName, 2).cache()

    val wordFrequencies = textData flatMap (_ split ("\\s+")
      map (word => (word, 1))) reduceByKey (_ + _)


    wordFrequencies.saveAsTextFile(outputDir)

    // Prints results
    wordFrequencies.takeOrdered(50)(Ordering[Int].reverse.on(_._2))
        .foreach(println)

    println("hello")
  }
}
