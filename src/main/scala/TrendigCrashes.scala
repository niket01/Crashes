import org.apache.spark.sql.expressions.Window.orderBy
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{count, current_timestamp, first, lit, row_number, sum, date_format}
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.pubsub.SparkPubsubMessage

import Convert.{extractCrashe, schema}

object TrendigCrashes {

  private def findTop20ZipCode(input: DataFrame) =
    input
      .where("ZIP_CODE != ''")
      .groupBy("ZIP_CODE")
      .agg(
        count("*").alias("count_crashes"),
        first("timestamp").alias("timestamp"),
        row_number().over(orderBy(count(("*")).desc)).alias("rank")
      )
      .select("rank", "ZIP_CODE", "count_crashes", "timestamp")
      .limit(20)

  private def findTop10Borough(input: DataFrame) =
    input
      .where("BOROUGH != ''")
      .groupBy("BOROUGH")
      .agg(
        count("*").alias("count_crashes"),
        first("timestamp").alias("timestamp"),
        row_number().over(orderBy(count(("*")).desc)).alias("rank")
      )
      .select("rank", "BOROUGH", "count_crashes", "timestamp")
      .limit(10)


  private def findCountInjured(input: DataFrame) =
    input
      .agg(
        sum("NUMBER_OF_PERSONS_INJURED").alias("count_injured"),
        sum("NUMBER_OF_PEDESTRIANS_INJURED").alias("count_pedestrians_injured"),
        sum("NUMBER_OF_CYCLIST_INJURED").alias("count_cyclist_injured"),
        sum("NUMBER_OF_MOTORIST_INJURED").alias("count_motorist_injured"),
        first("timestamp").alias("timestamp")
      )

  def processTrendingCrashes(stream: DStream[SparkPubsubMessage], windowInterval: Int, slidingInterval: Int,
                             spark: SparkSession) = {
    stream.window(Seconds(windowInterval), Seconds(slidingInterval))
      .foreachRDD {
        rdd =>
          val crashDF = spark.createDataFrame(extractCrashe(rdd), schema)
            .withColumn("timestamp", lit(date_format(current_timestamp(), "dd.MM.yyyy_hh-mm")))
              .cache()

          findCountInjured(crashDF).write.format("bigquery").option("table", "ratings.injured")
            .option("temporaryGcsBucket","crashes_bucket").mode(SaveMode.Append).save()
          findTop20ZipCode(crashDF).write.format("bigquery").option("table", "ratings.rating_zip_code")
            .option("temporaryGcsBucket","crashes_bucket").mode(SaveMode.Append).save()
          findTop10Borough(crashDF).write.format("bigquery").option("table", "ratings.rating_borough")
            .option("temporaryGcsBucket","crashes_bucket").mode(SaveMode.Append).save()

          crashDF.write.mode(SaveMode.Append).partitionBy("timestamp")
            .parquet("gs://crashes_bucket/data/")
      }
  }
}
