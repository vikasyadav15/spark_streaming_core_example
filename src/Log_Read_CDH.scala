/**
  * Created by vikasyadav on 3/20/17.
  */

import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext._;

object Log_Read_CDH  {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setMaster("local[2]").setAppName("Log process");
    val sc = new SparkContext(conf);
    val input = sc.textFile("/Users/vikasyadav/Music/HADOOP_SETUP/Cloudera_Spark_Training/intro-to-spark/spark-workshop-data/logs/access.log");
    //val word =input.flatMap(input=>input.split(" ").(0),input).filter(_.contains("/health")).count()
     val word=input.map(input=>(input.split(" ")(0),input))

    //word.take(10).foreach(println)
    println(word.first())

  }

}
