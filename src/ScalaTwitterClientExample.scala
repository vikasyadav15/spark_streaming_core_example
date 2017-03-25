import twitter4j.TwitterFactory
import twitter4j.Twitter
import twitter4j.conf.ConfigurationBuilder

object ScalaTwitterClientExample {

  def main(args : Array[String]) {


    val consumerKey="th49GUVdDxJbjy25A5p0ivA1n"
    val consumerSecret="9a72h80iLTf2qzXHEtA442eozBuD8VmsmQAO9DwQVDaLs3uJ24"
    val accessToken="1333434458-NwjSNUNnzRRrnC1Kq5gZ3xg0OBjHKDOBuDkJ3zt"
    val accessSecret="8IHJLtY1srvfij9byBLlVYcNeUpDnCyypvNPTlD0pefku"
    // (1) config work to create a twitter object
    val cb = new ConfigurationBuilder()
    cb.setDebugEnabled(true)
      .setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessSecret)
    val tf = new TwitterFactory(cb.build())
    val twitter = tf.getInstance()

    // (2) use the twitter object to get your friend's timeline
    val statuses = twitter.getFriendsTimeline()
    System.out.println("Showing friends timeline.")
    val it = statuses.iterator()
    while (it.hasNext()) {
      val status = it.next()
      println(status.getUser().getName() + ":" +
        status.getText());
    }

  }
}