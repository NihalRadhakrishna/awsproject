from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import boto3
import base64
import json
# ---------- CONFIG ----------
KAFKA_BOOTSTRAP = "b-3.uberkafkacluster.9wk1ik.c9.kafka.us-east-1.amazonaws.com:9096,b-2.uberkafkacluster.9wk1ik.c9.kafka.us-east-1.amazonaws.com:9096,b-1.uberkafkacluster.9wk1ik.c9.kafka.us-east-1.amazonaws.com:9096"
TOPIC = "uber-topic"
SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:143063372261:Uber-notification"
REGION = "us-east-1"

# ---------- SPARK ----------
spark = SparkSession.builder \
    .appName("UberStreamingAlerts") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------- SNS ----------
sns = boto3.client("sns", region_name=REGION)

def send_sns(msg):
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=msg,
        Subject="Uber Streaming Alert"
    )

# ---------- SCHEMA ----------
schema = """
Date STRING,
Time STRING,
Booking_ID STRING,
Booking_Status STRING,
Customer_ID STRING,
Vehicle_Type STRING,
Pickup_Location STRING,
Drop_Location STRING,
Avg_VTAT DOUBLE,
Avg_CTAT DOUBLE,
Cancelled_Rides_by_Customer INT,
Reason_for_cancelling_by_Customer STRING,
Cancelled_Rides_by_Driver INT,
Driver_Cancellation_Reason STRING,
Incomplete_Rides INT,
Incomplete_Rides_Reason STRING,
Booking_Value DOUBLE,
Ride_Distance DOUBLE,
Driver_Ratings DOUBLE,
Customer_Rating DOUBLE,
Payment_Method STRING,
Driver_ID STRING
"""

# ---------- READ KAFKA ----------
secret_name = "AmazonMSK_uber_final"
region_name = "us-east-1"

sm_client = boto3.client("secretsmanager", region_name=region_name)
secret_value = sm_client.get_secret_value(SecretId=secret_name)["SecretString"]
secret_dict = json.loads(secret_value)

username = secret_dict["username"]
password = secret_dict["password"]


raw_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "latest") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
    .option("kafka.sasl.jaas.config", f"org.apache.kafka.common.security.scram.ScramLoginModule required username=\"{username}\" password=\"{password}\";") \
    .load()


json_df = raw_df.selectExpr("CAST(value AS STRING)")

data_df = json_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# create event_time column
stream_df = data_df.withColumn(
    "event_time",
    current_timestamp()
)

# ---- DEDUPLICATION ----
stream_df = stream_df \
    .withWatermark("event_time", "10 minutes") \
    .dropDuplicates(["Booking_ID"])

# ------------------------------------------------
# CANCELLATION ALERT – 1 MIN WINDOW (ALL RIDES)
# ------------------------------------------------
cancel_df = stream_df.groupBy(
    window(col("event_time"), "1 minute")
).agg(
    count("*").alias("total_rides"),
    sum("Cancelled_Rides_by_Driver").alias("driver_cancel"),
    sum("Incomplete_Rides").alias("incomplete")
)

cancel_metric_df = cancel_df.withColumn(
    "cancel_percent",
    when(col("total_rides") != 0,
         (coalesce(col("driver_cancel"), lit(0)) +
          coalesce(col("incomplete"), lit(0))) / col("total_rides") * 100
    ).otherwise(0)
)

def cancel_batch(batch_df, batch_id):
    rows = batch_df.collect()
    for r in rows:
        if r["cancel_percent"] and r["cancel_percent"] > 3:
            send_sns(f"High Cancellation Rate: {r['cancel_percent']:.2f}% in last 1 minute")

# ------------------------------------------------
# HIGH VALUE / KM – 5 MIN WINDOW
# ------------------------------------------------
completed_df = stream_df.filter(col("Booking_Status") == "Completed")

value_df = completed_df.groupBy(
    window(col("event_time"), "5 minutes")
).agg(
    sum("Booking_Value").alias("sum_value"),
    sum("Ride_Distance").alias("sum_distance")
)

value_metric_df = value_df.withColumn(
    "value_per_km",
    when(col("sum_distance") != 0,
         col("sum_value") / col("sum_distance")
    ).otherwise(0)
)


def value_batch(batch_df, batch_id):
    rows = batch_df.collect()
    for r in rows:
        if r["value_per_km"] and r["value_per_km"] > 50:
            send_sns(f"High Value per KM Alert: {r['value_per_km']:.2f}")

# ------------------------------------------------
# START STREAMS
# ------------------------------------------------

q1 = cancel_metric_df.writeStream \
    .outputMode("update") \
    .foreachBatch(cancel_batch) \
    .start()

q2 = value_metric_df.writeStream \
    .outputMode("update") \
    .foreachBatch(value_batch) \
    .start()

spark.streams.awaitAnyTermination()
