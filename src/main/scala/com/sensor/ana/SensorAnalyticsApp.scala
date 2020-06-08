package com.sensor.ana

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SensorAnalyticsApp {
  val BasePath = "src/test/resources"

  def main(args: Array[String]) = {

    val sparkSession = SparkSession
      .builder()
      .appName("SensorAnalyticsApp")
      .master("local[4]")
      .getOrCreate()

    val df = sparkSession
      .read
      .format("json")
      .load(s"${BasePath}/ADDIT1ZB_SRES_SVR002FLW_C_20200428_155139.json")

    val explodedDf = df.select(explode(col("Items")).as("items"))

    val sensorDf = explodedDf
      .select(
        "items.Timestamp",
        "items.Value",
        "items.UnitsAbbreviation",
        "items.Good",
        "items.Questionable",
        "items.Substituted"
      )

    println(sensorDf.count())
    sensorDf.show
  }
}
