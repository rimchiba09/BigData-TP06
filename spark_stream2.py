from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)

spark = (
    SparkSession.builder
    .appName("CampusProcessingRateTest")
    .master("local[1]")
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.default.parallelism", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

class ProcessingRateListener(StreamingQueryListener):

    def onQueryStarted(self, event):
        print("Streaming query started.")

    def onQueryProgress(self, event):
        progress = event.progress

        input_rows = progress.numInputRows
        duration = progress.durationMs.get("triggerExecution", 0)

        if duration > 0:
            processing_rate = input_rows / (duration / 1000)

            print("Processed events:", input_rows)
            print("Batch duration:", duration, "ms")
            print("Processing rate:", round(processing_rate, 2), "events/second")

    def onQueryTerminated(self, event):
        print("Streaming query terminated.")

spark.streams.addListener(ProcessingRateListener())


schema = StructType([
    StructField("eventId", IntegerType(), True),
    StructField("studentId", StringType(), True),
    StructField("eventType", StringType(), True),
    StructField("department", StringType(), True),
    StructField("eventTime", StringType(), True)
])

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9093")
    .option("subscribe", "campus-events")
    .option("startingOffsets", "latest")
    .load()
)

json_events = raw.selectExpr("CAST(value AS STRING) AS json")

parsed = (
    json_events
    .select(from_json(col("json"), schema).alias("e"))
    .select("e.*")
    .withColumn("event_ts", to_timestamp(col("eventTime")))
    .filter(col("event_ts").isNotNull())
)

counts = (
    parsed
    .filter(col("eventType") == "check-in")
    .groupBy(
        window(col("event_ts"), "2 minutes", "1 minute"),
        col("department")
    )
    .count()
)

query = (
    counts.writeStream
    .outputMode("update")
    .format("console")
    .trigger(processingTime="5 seconds")
    .option("truncate", "false")
    .option("checkpointLocation", "./checkpoint/campus-events-rate-test")
    .start()
)

query.awaitTermination()
