/**
  * Created by vikasyadav on 1/26/17.
  */
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.StreamingContext._

object Spark_Twitter_Streaming  extends App{
  val conf = new SparkConf().setMaster("local[*]").setAppName("example")
             // .setJars(Array("/Users/vikasyadav/Music/HADOOP_SETUP/spark-2.0.2-bin-hadoop2.7/jars/twitter4j-core-2.2.3.jar","/Users/vikasyadav/Music/HADOOP_SETUP/spark-2.0.2-bin-hadoop2.7/jars/twitter4j-stream-3.0.3.jar"))
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")
  val ssc = new StreamingContext( sc,Seconds(1))

   // var  cb = new ConfigurationBuilder();
    //cb.setDebugEnabled(true).setOAuthConsumerKey("th49GUVdDxJbjy25A5p0ivA1n").setOAuthConsumerSecret("9a72h80iLTf2qzXHEtA442eozBuD8VmsmQAO9DwQVDaLs3uJ24").setOAuthAccessToken("1333434458-NwjSNUNnzRRrnC1Kq5gZ3xg0OBjHKDOBuDkJ3zt").setOAuthAccessTokenSecret("8IHJLtY1srvfij9byBLlVYcNeUpDnCyypvNPTlD0pefku")

//    val auth = Some(new OAuthAuthorization(cb.build()))
  //  val tweets = TwitterUtils.createStream(ssc, auth)
  System.setProperty("twitter4j.oauth.consumerKey", "*******")
  System.setProperty("twitter4j.oauth.consumerSecret", "*****")
  System.setProperty("twitter4j.oauth.accessToken", "1333434458-***")
  System.setProperty("twitter4j.oauth.accessTokenSecret", "****")
  val tweets = TwitterUtils.createStream(ssc, None)
  val statuses = tweets.map(status => status.getText())
  statuses.print()

  ssc.start()
  ssc.awaitTermination()

}
