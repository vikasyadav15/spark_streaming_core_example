package vikas.test

/**
  * Created by vikasyadav on 1/2/17.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Wordcount {
  def main(args: Array[String]) {

    //Create conf object
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local[*]")

    //create spark context object
    val sc = new SparkContext(conf)


    //Read file and create RDD
    val rawData = sc.textFile("/Users/vikasyadav/Music/data/d");

    //convert the lines into words using flatMap operation
    //val words = rawData.flatMap(line => line.split(" "))

    //count the individual words using map and reduceByKey operation
    //val wordCount = words.map(word => (word, 1)).reduceByKey(_ + _)
    //val pattern = "((,|;|'|\"|=|:|~|\\s|\\(|\\{|\\[|\\/|\\|\\*|#|\\^A|\\<[A-Za-z]*\\>)|^)([_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,}))((\\.|,|;|\\s|\\)|\\}|\\]|\\||\"|'|\\^A|\\<\\\\[A-Za-z]*\\>)|$)".r
  //  var str1= rawData.map(rawData=>rawData.toString)
  //  println(str1.getClass.getSimpleName)
   // println((pattern findAllIn str1).mkString(","))


    val processedRDD = rawData.map{
      case (inString) =>
//        val brandRegEx = "((,|;|'|\"|=|:|~|\\s|\\(|\\{|\\[|\\/|\\|\\*|#|\\^A|\\<[A-Za-z]*\\>)|^)([_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,}))((\\.|,|;|\\s|\\)|\\}|\\]|\\||\"|'|\\^A|\\<\\\\[A-Za-z]*\\>)|$)".r;
        val brandRegEx = "((,|;|'|\"|=|:|~|\\s|\\(|\\{|\\[|\\/|\\|\\*|#|\\^A|\\<[A-Za-z]*\\>)|^|\\001)([_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,}))((\\.|,|;|\\s|\\)|\\}|\\]|\\||\"|'|\\^A|\\<\\\\[A-Za-z]*\\>|\\001)|$)".r;

        //val brandRegEx = """(Marisha.*)""".r
        println("Input value++++++++++++++++++++++")
        println(inString)
        var brand = brandRegEx.findAllIn(inString)
        //brand
        println("output value++++++++++++++++++++++")
        brand.foreach(println)


    }

    processedRDD.collect().foreach(println)


    //Save the result
  //  rawData.saveAsTextFile("/Users/vikasyadav/Music/HADOOP_SETUP/spark-2.0.2-bin-hadoop2.7/sbin/vikas")

    //stop the spark context
    sc.stop
  }
}