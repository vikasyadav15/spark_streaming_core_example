/**
  * Created by vikasyadav on 1/26/17.
  */
 import org.apache.spark.{SparkConf, SparkContext};
object HelloTest {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Vikas").setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    sc.getConf.getAll.foreach(println)
    println(conf.toDebugString)
   /*SparkContext.sc._conf*/
//DEF
    val logFile = "/Users/vikasyadav/Music/HADOOP_SETUP/spark-2.0.2-bin-hadoop2.7/README.md"
    val line = sc.textFile(logFile);
    //val errr   =line.filter(line=>line.contains("Configuration"))
   // line.foreach(println);
   // errr.foreach(println);

    //val rdd1 = sc.parallelize(Seq((1, "Aug", 30),(1, "Sep", 31),(2, "Aug", 15),(2, "Sep", 10)))
    //val rdd2 = sc.parallelize(Seq((1, "Oct", 10),(1, "Nov", 12),(2, "Oct", 5),(2, "Nov", 15)))
    //rdd1.union(rdd2).collect.foreach(println)

    val a =sc.parallelize(List(1,2,3));
   // val b =a.METHOS(a=>a*a)
   // b.collect().foreach(print)

    val lines = sc.parallelize(List("hello world", "hi"))
    val words = lines.flatMap(line => line.split(" "))
    val words1 = lines.map(line => line.split(" "))
    words.first().foreach(println)  // returns "hello"
    println("teststst")
    words1.first().foreach(println)

//getMatchesNoReference(rdd)

  }
}

//publishprofileservice impl