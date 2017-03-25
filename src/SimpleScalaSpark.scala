
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


/**
  * Created by vikasyadav on 1/26/17.
  */
object SimpleScalaSpark {

  def main(args: Array[String]) {
    val logFile = "/Users/vikasyadav/Music/HADOOP_SETUP/spark-2.0.2-bin-hadoop2.7/README.md" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }

}
