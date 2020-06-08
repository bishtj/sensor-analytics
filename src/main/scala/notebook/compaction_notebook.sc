
import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder()
  .appName("SensorAnalyticsApp")
  .getOrCreate()

// Notebook code starts from here

spark.conf.set(
  "fs.azure.account.key.blobstorageac.blob.core.windows.net",
  "someKey")

val df = spark.read.format("json").load("wasbs://blobcontainer@blobstorageac.blob.core.windows.net/sourcedata/OSI/PI/COMPACTION-FILES/2020/04/16/*.json")

df.coalesce(5).write.format("json").save("wasbs://blobcontainer@blobstorageac.blob.core.windows.net/sourcedata/OSI/PI/COMPACTION-OUTPUT")


