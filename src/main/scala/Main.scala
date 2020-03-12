import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window.orderBy
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.pubsub.{PubsubUtils, SparkGCPCredentials}

object Main {
  case class Crashes(
                      CRASH_DATE: String,
                      CRASH_TIME: String,
                      BOROUGH: String,
                      ZIP_CODE: String,
                      LATITUDE: String,
                      LONGITUDE: String,
                      LOCATION: String,
                      ON_STREET_NAME: String,
                      CROSS_STREET_NAME: String,
                      OFF_STREET_NAME: String,
                      NUMBER_OF_PERSONS_INJURED: Int,
                      NUMBER_OF_PERSONS_KILLED: Int,
                      NUMBER_OF_PEDESTRIANS_INJURED: Int,
                      NUMBER_OF_PEDESTRIANS_KILLED: Int,
                      NUMBER_OF_CYCLIST_INJURED: Int,
                      NUMBER_OF_CYCLIST_KILLED: Int,
                      NUMBER_OF_MOTORIST_INJURED: Int,
                      NUMBER_OF_MOTORIST_KILLED: Int,
                      CONTRIBUTING_FACTOR_VEHICLE_1: String,
                      CONTRIBUTING_FACTOR_VEHICLE_2: String,
                      CONTRIBUTING_FACTOR_VEHICLE_3: String,
                      CONTRIBUTING_FACTOR_VEHICLE_4: String,
                      CONTRIBUTING_FACTOR_VEHICLE_5: String,
                      COLLISION_ID: String,
                      VEHICLE_TYPE_CODE_1: String,
                      VEHICLE_TYPE_CODE_2: String,
                      VEHICLE_TYPE_CODE_3: String,
                      VEHICLE_TYPE_CODE_4: String,
                      VEHICLE_TYPE_CODE_5: String
                    )

  def main(args: Array[String]) : Unit = {
    val projectID = "igneous-equinox-269508"
    val slidingInterval: Int = 5*60

    val appName = "SparkCrashes"

    val conf = new SparkConf().setMaster("local[2]").setAppName(appName)
    val ssc = new StreamingContext(conf, Seconds(slidingInterval))

    val spark = SparkSession
      .builder()
      .appName(appName)
      .config(conf)
      .getOrCreate()

    import spark.implicits._

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
        val crashDF = rdd.map(_.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"))
            .map{
              case Array(
              s0,
              s1,
              s2,
              s3,
              s4,
              s5,
              s6,
              s7,
              s8,
              s9,
              s10,
              s11,
              s12,
              s13,
              s14,
              s15,
              s16,
              s17,
              s18,
              s19,
              s20,
              s21,
              s22,
              s23,
              s24,
              s25,
              s26,
              s27,
              s28
              ) =>
                Crashes(
                  s0,
                  s1,
                  s2,
                  s3,
                  s4,
                  s5,
                  s6,
                  s7,
                  s8,
                  s9,
                  s10.toInt,
                  s11.toInt,
                  s12.toInt,
                  s13.toInt,
                  s14.toInt,
                  s15.toInt,
                  s16.toInt,
                  s17.toInt,
                  s18,
                  s19,
                  s20,
                  s21,
                  s22,
                  s23,
                  s24,
                  s25,
                  s26,
                  s27,
                  s28
                )
            }
          .toDF()

        val newCrashDF = crashDF.withColumn("timestamp", current_timestamp())

        val top20_ZIP_CODE =
        newCrashDF
          .where("ZIP_CODE != ''")
          .groupBy("ZIP_CODE")
          .agg(
            count("*").alias("count_crashes"),
            first("timestamp").alias("timestamp"),
            row_number().over(orderBy(count(("*")).desc)).alias("rank")
          )
          .select("rank", "ZIP_CODE", "count_crashes", "timestamp")
          .limit(20)

        val top10_BOROUGH =
          newCrashDF
            .where("BOROUGH != ''")
            .groupBy("BOROUGH")
            .agg(
              count("*").alias("count_crashes"),
              first("timestamp").alias("timestamp"),
              row_number().over(orderBy(count(("*")).desc)).alias("rank")
            )
            .select("rank", "BOROUGH", "count_crashes", "timestamp")
            .limit(10)

        /* здесь должны быть пострадавшие пассажиры */

        /* коннектор к big query + импорт данных в созданные таблицы */

        newCrashDF.write.parquet("gs://crashes_bucket/data/" + current_timestamp().toString())
        top20_ZIP_CODE.write.parquet("gs://crashes_bucket/top20_ZIP_CODE/" + current_timestamp().toString())
        top10_BOROUGH.write.parquet("gs://crashes_bucket/top10_BOROUGH/" + current_timestamp().toString())
    }

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }
}
