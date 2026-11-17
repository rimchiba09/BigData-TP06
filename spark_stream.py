
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)

# spark = (
#     SparkSession.builder
#     .appName("CampusRealTimeAnalytics")
#     .getOrCreate()
# )

# spark.sparkContext.setLogLevel("WARN")

spark = (
    SparkSession.builder
    .appName("CampusRealTimeAnalytics")
    .master("local[1]")
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.default.parallelism", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


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
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "campus-events")
    .option("startingOffsets", "earliest")
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

checkins = parsed.filter(col("eventType") == "check-in")

counts = (
    checkins
    # .withWatermark("event_ts", "30 seconds")
    .withWatermark("event_ts", "2 minutes")
    .groupBy(
        # window(col("event_ts"), "1 minute"),
        # window(col("event_ts"), "2 minute"),
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
    # .option("checkpointLocation", "./checkpoint/campus-events")

    # .option("checkpointLocation", "./checkpoint/campus-events-trigger5")
    # .option("checkpointLocation", "./checkpoint/campus-events-window2")
    # .option("checkpointLocation", "./checkpoint/campus-events-sliding")
    .option("checkpointLocation", "./checkpoint/campus-events-watermark2")
    .start()
)

query.awaitTermination()
