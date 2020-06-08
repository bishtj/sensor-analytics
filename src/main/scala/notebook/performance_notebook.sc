
import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder()
  .appName("SensorAnalyticsApp")
  .master("local[4]")
  .getOrCreate()

// Notebook code starts from here

import org.apache.spark.sql.functions._

spark.conf.set(
  "fs.azure.account.key.blobstorageac.blob.core.windows.net",
  "someKey")

//val df = spark.read.format("json").load("wasbs://blobcontainer@blobstorageac.blob.core.windows.net/sourcedata/OSI/PI/ADDIT1ZB_SRES_SVR002FLW_C/2020/04/16/*.json")
val df = spark
  .read
  .format("json")
  .load("wasbs://blobcontainer@blobstorageac.blob.core.windows.net/sourcedata/OSI/PI/*/2020/04/16/*C_20200428_155139.json")

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

println("Total num records " + sensorDf.count())
