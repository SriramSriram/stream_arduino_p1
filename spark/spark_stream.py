from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ✅ Create Spark Session with Kafka support
spark = SparkSession.builder \
    .appName("SensorStream") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.postgresql:postgresql:42.7.3") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ✅ Schema (same as producer)
schema = StructType([
    StructField("distance", FloatType()),
    StructField("avg_distance", FloatType()),
    StructField("status", StringType()),
    StructField("event_time", LongType()),
    
])

# ✅ Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-data") \
    .load()

# ✅ Convert JSON
json_df = df.selectExpr("CAST(value AS STRING)")

parsed = json_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# ✅ Convert timestamp (milliseconds → timestamp)
parsed = parsed.withColumn(
    "event_time",
    (col("event_time") / 1000).cast("timestamp")
)

# 🔥 FUNCTION: Write to PostgreSQL
def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://127.0.0.1:5433/sensordb") \
        .option("dbtable", "sensor_data") \
        .option("user", "postgres") \
        .option("password", "1234") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# ✅ STREAM → PostgreSQL
query = parsed.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

# ✅ ALSO print in console (for debugging)
console_query = parsed.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

spark.streams.awaitAnyTermination()