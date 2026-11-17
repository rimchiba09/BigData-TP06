
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp,
    hour, minute, second, date_format,
    concat, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)

spark = (
    SparkSession.builder
    .appName("CampusLateCheckInAlert")
    .master("local[1]")
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.default.parallelism", "2")
    .config("spark.sql.session.timeZone", "Africa/Algiers")
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

parsed = (
    raw.selectExpr("CAST(value AS STRING) AS json")
    .select(from_json(col("json"), schema).alias("e"))
    .select("e.*")
    .withColumn("event_ts", to_timestamp(col("eventTime")))
    .filter(col("event_ts").isNotNull())
)

late_checkins = (
    parsed
    .filter(col("eventType") == "check-in")
    .filter(
        (hour(col("event_ts")) > 8) |
        ((hour(col("event_ts")) == 8) & (minute(col("event_ts")) > 15)) |
        (
            (hour(col("event_ts")) == 8) &
            (minute(col("event_ts")) == 15) &
            (second(col("event_ts")) > 0)
        )
    )
    .select(
        col("studentId"),
        col("department"),
        date_format(
            col("event_ts"), "yyyy-MM-dd HH:mm:ss"
        ).alias("eventTime"),
        concat(
            lit("LATE CHECK-IN ALERT! Student "),
            col("studentId"),
            lit(" from "),
            col("department"),
            lit(" checked in late at "),
            date_format(col("event_ts"), "yyyy-MM-dd HH:mm:ss")
        ).alias("alertMessage")
    )
)

query = (
    late_checkins.writeStream
    .outputMode("append")
    .format("console")
    .trigger(processingTime="5 seconds")
    .option("truncate", "false")
    .option(
        "checkpointLocation",
        "./checkpoint/campus-late-alert-message"
    )
    .start()
)

query.awaitTermination()
