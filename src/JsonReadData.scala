import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
// Import statement to implicitly convert an RDD to a DataFrame


object JsonReadData {

  def main(args: Array[String]) {
    //val logFile = "/Users/vikasyadav/artist.json" // Should be some file on your system
    val logFile ="/Users/vikasyadav/s3_file/CRF/LT-*.log"
    val conf = new SparkConf().setAppName("Simple Application Json").setMaster("local[*]")
    val sc = new SparkContext(conf)
    /*val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._*/

    //val logData = sc.wholeTextFiles(logFile).map(x => x._2)

    /*print("vikasdlkndklasndsndn")
    //logData.collect();
    //case class Artist ( id :Int,  gid:String,  name:String,  sort_name:String,  begin_date_year:Int,  begin_date_month:Int,  begin_date_day:Int,  end_date_year:Int,  end_date_month:Int,  end_date_day:Int,  type1:Int,  area:Int,  gender:Int,  comment:String,  edits_pending:Int,  last_updated:String,  ended:String,  begin_area:Int,  end_area:Int)
    case class Artist ( id :String,  gid:String,  name:String, begin_date_year:String)
    val employees = (logData.map(logData=>( Artist(logData(0), logData(1), logData(2), logData(3) )))).toDF()
    //employees.registerTempTable("people")
    employees.registerTempTable("employees1")
    print("vikasdlkndklasndsndn")
    //employees.toDF.count();
    employees1.printSchema()
    //employees.select(logData(0)).take(100).foreach(println)
    //print("vikasdlkndklasndsndn")
    //employees.collect();
   /* val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val dfs = sqlContext.read.json(logFile)
    print("vikasdlkndklasndsndn")
    dfs.take(100).foreach(println)*/*/

    val logData = sc.textFile(logFile).map(x =>( x.charAt(0),x))
    val ter=logData.groupByKey().mapValues(x=>x.toSet.size).first()
    sc.parallelize(Seq(ter)).collect().foreach(println)
    print(ter.getClass)
            // ter.collect().foreach(println)

    readLine()



  }

}
