package scala

import java.nio.charset.StandardCharsets

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.streaming.pubsub.SparkPubsubMessage

object Convert {

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
  private def convertToInt(r: String) = {
    try {
      r.toInt
    } catch {
      case e: Exception => 0
    }
  }

  private def nvl(r: String) = {
    if (r.length() == 0) null else r
  }

  private def nvlList(r: List[String]) = {r.map(r => nvl(r))}

  def extractCrashe(input: RDD[SparkPubsubMessage]) = {
    input.map(message => new String(message.getData(), StandardCharsets.UTF_8))
      .filter(_.length != 0)
      .map(_.split(""",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"""))
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
}
