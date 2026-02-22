from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import boto3
import json
from pyspark.sql.types import MapType, StringType

# ---------- CONFIG ----------
KAFKA_BOOTSTRAP = "b-3.uberkafkacluster.9wk1ik.c9.kafka.us-east-1.amazonaws.com:9096," \
                  "b-2.uberkafkacluster.9wk1ik.c9.kafka.us-east-1.amazonaws.com:9096," \
                  "b-1.uberkafkacluster.9wk1ik.c9.kafka.us-east-1.amazonaws.com:9096"
TOPIC = "uber-topic"
S3_PATH = "s3://uber-project1.1/spark-batch/bronze/"
REGION = "us-east-1"

# ---------- SPARK ----------
spark = SparkSession.builder \
    .appName("UberKafkaBatchRawToS3") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------- SECRET MANAGER ----------
secret_name = "AmazonMSK_uber_final"
sm_client = boto3.client("secretsmanager", region_name=REGION)
secret_value = sm_client.get_secret_value(SecretId=secret_name)["SecretString"]
secret_dict = json.loads(secret_value)

username = secret_dict["username"]
password = secret_dict["password"]

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
Avg_VTAT STRING, 
Avg_CTAT STRING, 
Cancelled_Rides_by_Customer STRING, 
Reason_for_cancelling_by_Customer STRING, 
Cancelled_Rides_by_Driver STRING, 
Driver_Cancellation_Reason STRING, 
Incomplete_Rides STRING, 
Incomplete_Rides_Reason STRING, 
Booking_Value STRING, Ride_Distance STRING, 
Driver_Ratings STRING, 
Customer_Rating STRING, 
Payment_Method STRING, 
Driver_ID STRING
"""

# ---------- READ KAFKA (BATCH / REPLAY) ----------
raw_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
    .option(
        "kafka.sasl.jaas.config",
        f'org.apache.kafka.common.security.scram.ScramLoginModule required '
        f'username="{username}" password="{password}";'
    ) \
    .load()

# ---------- PARSE JSON ----------
json_df = raw_df.selectExpr("CAST(value AS STRING) as json_str")

data_df = json_df.select(
    from_json(col("json_str"), schema).alias("data")
).select("data.*")


# ---------- WRITE TO S3 (BRONZE) ----------
data_df.write \
    .mode("append") \
    .parquet(S3_PATH)


