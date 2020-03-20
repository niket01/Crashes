import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
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
  def convertToInt(r: String) = {
    try {
      r.toInt
    } catch {
      case e: Exception => 0
    }
  }

  def nvl(r: String) = {
    if (r.length() == 0) null else r
  }

  def nvlList(r: List[String]) = {r.map(r => nvl(r))}

  private def extractCrashe(input: RDD[String]) = {
    input.map(_.split(""",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"""))
      .filter(_.length != 0)
      .map {
        attribute =>
          nvlList(attribute.take(10).toList) :::
            List(convertToInt(attribute(10)),
              convertToInt(attribute(11)),
              convertToInt(attribute(12)),
              convertToInt(attribute(13)),
              convertToInt(attribute(14)),
              convertToInt(attribute(15)),
              convertToInt(attribute(16)),
              convertToInt(attribute(17))
            ) :::
            nvlList(attribute.takeRight(11).toList)
      }
      .map(attribute => Row.fromSeq(attribute))
  }

  def main(args: Array[String]) : Unit = {
    val projectID = "igneous-equinox-269508"
    val slidingInterval: Int = 60

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
        StructField("NUMBER_OF_PERSONS_INJURED", IntegerType, nullable = false),
        StructField("NUMBER_OF_PERSONS_KILLED", IntegerType, nullable = false),
        StructField("NUMBER_OF_PEDESTRIANS_INJURED", IntegerType, nullable = false),
        StructField("NUMBER_OF_PEDESTRIANS_KILLED", IntegerType, nullable = false),
        StructField("NUMBER_OF_CYCLIST_INJURED", IntegerType, nullable = false),
        StructField("NUMBER_OF_CYCLIST_KILLED", IntegerType, nullable = false),
        StructField("NUMBER_OF_MOTORIST_INJURED", IntegerType, nullable = false),
        StructField("NUMBER_OF_MOTORIST_KILLED", IntegerType, nullable = false),
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

    messagesStream.window(Seconds(5*60), Seconds(slidingInterval))
      .foreachRDD{
        rdd =>
          val crashDF = spark.createDataFrame(extractCrashe(rdd), schema)

          val newCrashDF = crashDF.withColumn("timestamp", lit(unix_timestamp())).cache()

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

           val count_injured = newCrashDF.agg(
             sum("NUMBER_OF_PERSONS_INJURED").alias("count_injured"),
             sum("NUMBER_OF_PEDESTRIANS_INJURED").alias("count_pedestrians_injured"),
             sum("NUMBER_OF_CYCLIST_INJURED").alias("count_cyclist_injured"),
             sum("NUMBER_OF_MOTORIST_INJURED").alias("count_motorist_injured"),
             first("timestamp").alias("timestamp")
           )

          count_injured.write.format("bigquery").option("table", "ratings.injured")
            .mode(SaveMode.Append).save()
          top20_ZIP_CODE.write.format("bigquery").option("table", "ratings.rating_zip_code")
            .mode(SaveMode.Append).save()
          top10_BOROUGH.write.format("bigquery").option("table", "ratings.rating_borough")
            .mode(SaveMode.Append).save()

          newCrashDF.write.mode(SaveMode.Append).partitionBy("timestamp")
            .parquet("gs://crashes_bucket/data/")
        }

    ssc.start()             // Start the computation
    //ssc.awaitTermination()  // Wait for the computation to terminate
    ssc.awaitTerminationOrTimeout(1000 * 60 * 60)
  }
}
