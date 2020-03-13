import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window.orderBy
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.pubsub.{PubsubUtils, SparkGCPCredentials}

object Main {
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

    val messagesStream: DStream[String] = PubsubUtils
      .createStream(
        ssc,
        projectID,
        None,
        "crashes_subscriptions",
        SparkGCPCredentials.builder.build(), StorageLevel.MEMORY_AND_DISK_SER_2)
        .map(message => new String(message.getData(), StandardCharsets.UTF_8))

    val schema = StructType(
      List(
        StructField("CRASH_DATE", StringType, nullable = true),
        StructField("CRASH_TIME", StringType, nullable = true),
        StructField("BOROUGH", StringType, nullable = true),
        StructField("ZIP_CODE", StringType, nullable = true),
        StructField("LATITUDE", StringType, nullable = true),
        StructField("LONGITUDE", StringType, nullable = true),
        StructField("LOCATION", StringType, nullable = true),
        StructField("ON_STREET_NAME", StringType, nullable = true),
        StructField("CROSS_STREET_NAME", StringType, nullable = true),
        StructField("OFF_STREET_NAME", StringType, nullable = true),
        StructField("NUMBER_OF_PERSONS_INJURED", StringType, nullable = true),
        StructField("NUMBER_OF_PERSONS_KILLED", StringType, nullable = true),
        StructField("NUMBER_OF_PEDESTRIANS_INJURED", StringType, nullable = true),
        StructField("NUMBER_OF_PEDESTRIANS_KILLED", StringType, nullable = true),
        StructField("NUMBER_OF_CYCLIST_INJURED", StringType, nullable = true),
        StructField("NUMBER_OF_CYCLIST_KILLED", StringType, nullable = true),
        StructField("NUMBER_OF_MOTORIST_INJURED", StringType, nullable = true),
        StructField("NUMBER_OF_MOTORIST_KILLED", StringType, nullable = true),
        StructField("CONTRIBUTING_FACTOR_VEHICLE_1", StringType, nullable = true),
        StructField("CONTRIBUTING_FACTOR_VEHICLE_2", StringType, nullable = true),
        StructField("CONTRIBUTING_FACTOR_VEHICLE_3", StringType, nullable = true),
        StructField("CONTRIBUTING_FACTOR_VEHICLE_4", StringType, nullable = true),
        StructField("CONTRIBUTING_FACTOR_VEHICLE_5", StringType, nullable = true),
        StructField("COLLISION_ID", StringType, nullable = true),
        StructField("VEHICLE_TYPE_CODE_1", StringType, nullable = true),
        StructField("VEHICLE_TYPE_CODE_2", StringType, nullable = true),
        StructField("VEHICLE_TYPE_CODE_3", StringType, nullable = true),
        StructField("VEHICLE_TYPE_CODE_4", StringType, nullable = true),
        StructField("VEHICLE_TYPE_CODE_5", StringType, nullable = true)
      )
    )

    messagesStream.foreachRDD{
      rdd =>
        val splitRDD = rdd.map(attribute => attribute.split(""",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"""))
          .map {
            attribute =>
              attribute.take(10).toList :::
              List(attribute(10),
                attribute(11),
                attribute(12),
                attribute(13),
                attribute(14),
                attribute(15),
                attribute(16),
                attribute(17)
              ) :::
              attribute.takeRight(11).toList
          }
          .map(attribute => Row.fromSeq(attribute))

              val crashDF = spark.createDataFrame(splitRDD, schema)

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

              newCrashDF.write.partitionBy("timestamp")
                .parquet("gs://crashes_bucket/data/")
              top20_ZIP_CODE.write.partitionBy("timestamp")
                .parquet("gs://crashes_bucket/top20_ZIP_CODE/")
              top10_BOROUGH.write.partitionBy("timestamp")
                .parquet("gs://crashes_bucket/top10_BOROUGH/")
          }

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }
}
