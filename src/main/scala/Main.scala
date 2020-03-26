package scala

import org.apache.spark.SparkConf

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.pubsub.{PubsubUtils, SparkGCPCredentials}

import scala.TrendigCrashes.processTrendingCrashes

object Main {
  def createContext(projectID: String, windowInterval: Int, slidingInterval: Int) = {
    val appName = "SparkCrashes"

    val conf = new SparkConf().setMaster("local[2]").setAppName(appName)
    val ssc = new StreamingContext(conf, Seconds(slidingInterval))

    val spark: SparkSession = SparkSession
      .builder()
      .appName(appName)
      .config(conf)
      .getOrCreate()

    val crashesStream = PubsubUtils
      .createStream(
        ssc,
        projectID,
        None,
        "crashes_subscriptions",
        SparkGCPCredentials.builder.build(), StorageLevel.MEMORY_AND_DISK_SER_2)

    processTrendingCrashes(crashesStream, windowInterval, slidingInterval, spark)

    ssc
  }

  def main(args: Array[String]) : Unit = {
    if (args.length != 4) {
      System.err.println(
        """
          | Usage: TrendingHashtags <projectID> <windowLength> <slidingInterval>
          |
          |     <projectID>: ID of Google Cloud project
          |     <windowLength>: The duration of the window, in seconds
          |     <slidingInterval>: The interval at which the window calculation is performed, in seconds.
          |     <totalRunningTime>: Total running time for the application, in minutes. If 0, runs indefinitely until termination.
          |
        """.stripMargin)
      System.exit(1)
    }

    val Seq(projectID, windowInterval, slidingInterval, totalRunningTime) = args.toSeq

    val ssc = createContext(projectID, windowInterval.toInt, slidingInterval.toInt)

    ssc.start()             // Start the computation

    if (totalRunningTime.toInt == 0) {
      ssc.awaitTermination()
    }
    else {
      ssc.awaitTerminationOrTimeout(1000 * 60 * totalRunningTime.toInt)
    }
  }
}
