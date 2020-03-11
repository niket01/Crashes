import java.nio.charset.StandardCharsets
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.pubsub.{PubsubUtils, SparkGCPCredentials}

object Main {
  def main(args: Array[String]) : Unit = {
    val totalRunningTime = "1"
    val projectID = "igneous-equinox-269508"
    val windowLength: Int = 20
    val slidingInterval: Int = 60

    val conf = new SparkConf().setMaster("local[2]").setAppName("SparkCrashes")
    val ssc = new StreamingContext(conf, Seconds(slidingInterval))

    val messagesStream: DStream[String] = PubsubUtils
      .createStream(
        ssc,
        projectID,
        None,
        "crashes_subscription",
        SparkGCPCredentials.builder.build(), StorageLevel.MEMORY_AND_DISK_SER_2)
      .map(message => new String(message.getData(), StandardCharsets.UTF_8))

    messagesStream.foreachRDD{
      rdd =>
        val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
        import spark.implicits._

        val format = "yyyyMMdd_HHmmss"
        val dtf = DateTimeFormatter.ofPattern(format)
        val ldt = LocalDateTime.now()

        val wordsDataFrame = rdd.toDF("word")
        wordsDataFrame.write.parquet("gs://crashes_bucket/data/" + ldt.format(dtf))
    }

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }
}
