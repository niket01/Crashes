import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SparkSession}
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

    messagesStream.foreachRDD{
      rdd =>
        val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
        import spark.implicits._

        val crash = rdd.toDF("CRASH_DATE,CRASH_TIME,BOROUGH,ZIP_CODE,LATITUDE,LONGITUDE,LOCATION," +
          "ON_STREET_NAME,CROSS_STREET_NAME,OFF_STREET_NAME,NUMBER_OF_PERSONS_INJURED,NUMBER_OF_PERSONS_KILLED," +
          "NUMBER_OF_PEDESTRIANS_INJURED,NUMBER_OF_PEDESTRIANS_KILLED,NUMBER_OF_CYCLIST_INJURED," +
          "NUMBER_OF_CYCLIST_KILLED,NUMBER_OF_MOTORIST_INJURED,NUMBER_OF_MOTORIST_KILLED," +
          "CONTRIBUTING_FACTOR_VEHICLE_1,CONTRIBUTING_FACTOR_VEHICLE_2,CONTRIBUTING_FACTOR_VEHICLE_3," +
          "CONTRIBUTING_FACTOR_VEHICLE_4,CONTRIBUTING_FACTOR_VEHICLE_5,COLLISION_ID,VEHICLE_TYPE_CODE_1," +
          "VEHICLE_TYPE_CODE_2,VEHICLE_TYPE_CODE_3,VEHICLE_TYPE_CODE_4,VEHICLE_TYPE_CODE_5")

        val view = crash.createTempView("Crashes")

        val sqlDF = spark.sql("select *, current_timestamp() as timestamp from view")

        val top20_ZIP_CODE = spark.sql("""
                                  select ZIP_CODE,
                                         count(*),
                                         rownum() partition by (ZIP_CODE order by count(*) desc) as rnk,
                                         first(timestamp)
                                    from Crashes
                                   where ZIP_CODE != ''
                                   group ZIP_CODE
                                   limit 20""")

        val top10_BOROUGH = spark.sql("""
                                  select BOROUGH,
                                         count(*),
                                         rownum() partition by (BOROUGH order by count(*) desc) as rnk,
                                         first(timestamp)
                                    from Crashes
                                   where BOROUGH != ''
                                   group BOROUGH
                                   limit 10""")

        /* здесь должны быть пострадавшие пассажиры */

        /* коннектор к big query + импорт данных в созданные таблицы */

        sqlDF.write.parquet("gs://crashes_bucket/data/")
    }

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }
}
