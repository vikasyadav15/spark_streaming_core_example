/**
  * Created by vikasyadav on 3/25/17.
  */
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import StreamingContext._
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.log4j.Logger
import org.apache.log4j.Level


object spark_in_Action_streaming_example {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    //StreamingExamples.setStreamingLogLevels()
    val sparkConf = new SparkConf().setAppName("buy_sell_count_by_hour_spark_streaming").setMaster("local[4]")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(6))
    val logFile = "/Users/vikasyadav/Code_Repo/spark_in_action/dstream_input/"

    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    val filestream = ssc.textFileStream(logFile)
    import java.sql.Timestamp
    case class Order(filename:String,time: java.sql.Timestamp, orderId:Long, clientId:Long, symbol:String, amount:Int, price:Double, buy:Boolean)

    import java.text.SimpleDateFormat
    val orders = filestream.flatMap(line => {
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
      val s = line.split(",")
      try {
        assert(s(7) == "B" || s(7) == "S")
        List(Order(s(0),new Timestamp(dateFormat.parse(s(1)).getTime()), s(2).toLong, s(3).toLong, s(4), s(5).toInt, s(6).toDouble, s(7) == "B"))
      }
      catch {
        case e : Throwable => println("Wrong line format ("+e+"): "+line)
          List()
      }
    })

    val numPerType = orders.map(o => (o.buy, 1L)).reduceByKey((c1, c2) => c1+c2)
    println("####################Step 1##########################################################")
    orders.print()
    orders.map(orders=>orders.filename).transform(rdd=> rdd.distinct).print()
    numPerType.print(10000000)
    println("####################Step 1##########################################################")

    //numPerType.repartition(1).saveAsTextFiles("/Users/vikasyadav/Code_Repo/spark_in_action/dstream_output/", "txt")
    val amountPerClient = orders.map(o => (o.clientId, o.amount*o.price))
    println("####################Step 2##########################################################")
    amountPerClient.print()
    println("####################Step 2##########################################################")
    val amountState = amountPerClient.updateStateByKey((vals, totalOpt:Option[Double]) => {
      totalOpt match {
        case Some(totalOpt) => Some(vals.sum + totalOpt)
        case None => Some(vals.sum)
      }
    })
    println("####################Step 2##########################################################")
    amountState.print()
    println("####################Step 2##########################################################")

    val top5clients = amountState.transform(_.sortBy(_._2, false).map(_._1).
      zipWithIndex.filter(x => x._2 < 5))
    println("####################Step 3##########################################################")
    top5clients.print()
    println("####################Step 2##########################################################")
    val buySellList = numPerType.map(t =>
      if(t._1) ("BUYS", List(t._2.toString))
      else ("SELLS", List(t._2.toString)) )
    val top5clList = top5clients.repartition(1).
      map(x => x._1.toString).
      glom().
      map(arr => ("TOP5CLIENTS", arr.toList))

    val finalStream = buySellList.union(top5clList)

    finalStream.repartition(1).saveAsTextFiles("/Users/vikasyadav/Code_Repo/spark_in_action/dstream_output/", "txt")

    ssc.checkpoint("/Users/vikasyadav/Code_Repo/spark_in_action/checkpoint/")

    ssc.start()
    ssc.awaitTermination()
  }
}
