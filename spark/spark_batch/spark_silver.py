from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ---------- CONFIG ----------
BRONZE_PATH = "s3://uber-project1.1/spark-batch/bronze/"
SILVER_PATH = "s3://uber-project1.1/spark-batch/silver/"

# ---------- SPARK ----------
spark = SparkSession.builder \
    .appName("UberSilverLayer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------- READ BRONZE ----------
bronze_df = spark.read.parquet(BRONZE_PATH)

# ---------- DATA QUALITY / CLEANING ----------


clean_df = bronze_df.filter(col("Booking_ID").isNotNull())


clean_df = clean_df.select([
    trim(col(c)).alias(c) if t == "string" else col(c)
    for c, t in clean_df.dtypes
])


numeric_cols = [
    "Booking_Value",
    "Ride_Distance",
    "Driver_Ratings",
    "Customer_Rating",
    "Avg_VTAT",
    "Avg_CTAT"
]

for c in numeric_cols:
    clean_df = clean_df.withColumn(
        c,
        when(
            col(c).rlike("^[0-9]+(\\.[0-9]+)?$"),
            col(c).cast("double")
        ).otherwise(None)
    )


for c in numeric_cols:
    clean_df = clean_df.withColumn(
        c,
        when(isnan(col(c)), None).otherwise(col(c))
    )


clean_df = clean_df.filter(
    (col("Booking_Value") >= 0) | col("Booking_Value").isNull()
)

clean_df = clean_df.filter(
    (col("Ride_Distance") >= 0) | col("Ride_Distance").isNull()
)


clean_df = clean_df.withColumn(
    "Date",
    to_date(col("Date"), "dd-MM-yyyy")
)

clean_df = clean_df.withColumn(
    "Time",
    date_format(to_timestamp(col("Time"), "HH:mm:ss"), "HH:mm:ss")
)

# ---------- DROP DUPLICATES (INTRA BATCH) ----------
dedup_df = clean_df.dropDuplicates(["Booking_ID"])

# ---------- IDEMPOTENCY (INTER BATCH) ----------
try:
    existing_silver = spark.read.parquet(SILVER_PATH)

    new_data = dedup_df.join(
        existing_silver.select("Booking_ID"),
        on="Booking_ID",
        how="left_anti"
    )

except:
    # First run
    new_data = dedup_df

# ---------- ADD PARTITION COLUMNS ----------
new_data = new_data \
    .withColumn("year", year(col("Date"))) \
    .withColumn("month", month(col("Date"))) \
    .withColumn("day", dayofmonth(col("Date")))

# ---------- WRITE SILVER ----------
new_data.write \
    .mode("append") \
    .partitionBy("month", "day") \
    .parquet(SILVER_PATH)
