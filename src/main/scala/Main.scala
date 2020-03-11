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
  case class Crash(CrashDate: String,
                   CrashTime: String,
                   BOROUGH: String,
                   ZIP_CODE: String,
                   PERSONS_INJURED: Int,
                   PERSONS_KILLED: Int,
                   PEDESTRIANS_INJURED: Int,
                   PEDESTRIANS_KILLED: Int,
                   CYCLIST_INJURED: Int,
                   CYCLIST_KILLED: Int,
                   MOTORIST_INJURED: Int,
                   MOTORIST_KILLED: Int
                  )

  def main(args: Array[String]) : Unit = {
    val projectID = "igneous-equinox-269508"
    val slidingInterval: Int = 60*60

    val conf = new SparkConf().setMaster("local[2]").setAppName("SparkCrashes")
    val ssc = new StreamingContext(conf, Seconds(slidingInterval))

    val messagesStream: DStream[String] = PubsubUtils
      .createStream(
        ssc,
        projectID,
        None,
        "crashes_subscriptions",
        SparkGCPCredentials.builder.build(), StorageLevel.MEMORY_AND_DISK_SER_2)
      .map(message => new String(message.getData(), StandardCharsets.UTF_8))

    messagesStream.foreachRDD{
      rdd =>
        val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
        import spark.implicits._

        val format = "yyyyMMdd"
        val dtf = DateTimeFormatter.ofPattern(format)
        val ldt = LocalDateTime.now()

        val split = rdd.map(_.split(","))
        val crash = split.map(r =>
          Crash(
            r(0),
            r(1),
            r(2),
            r(3),
            r(10).toInt,
            r(11).toInt,
            r(12).toInt,
            r(13).toInt,
            r(14).toInt,
            r(15).toInt,
            r(16).toInt,
            r(17).toInt
          )
        )

        crash.toDF().write.parquet("gs://crashes_bucket/data/" + ldt.format(dtf))
    }

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }
}
