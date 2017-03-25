package vikas.test

/**
  * Created by vikasyadav on 1/2/17.
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
object te {
  def main(args: Array[String]) {

    //Create conf object
    val conf = new SparkConf()
      .setAppName("te")
      .setMaster("local[*]")

    //create spark context object
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .option("delimiter", "\u0001")
      .load("/Users/vikasyadav/Music/data/tes")

    df.show(20,false)
    df.printSchema()
    df.take(10)


  }
}