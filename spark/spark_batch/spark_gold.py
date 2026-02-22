from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ---------- CONFIG ----------
SILVER_PATH = "s3://uber-project1.1/spark-batch/silver/"
GOLD_PATH = "s3://uber-project1.1/spark-batch/gold/"

# ---------- SPARK ----------
spark = SparkSession.builder \
    .appName("UberGoldLayer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------- READ SILVER ----------
silver_df = spark.read.parquet(SILVER_PATH)

# ---------- METRICS / AGGREGATIONS ----------

silver_df = silver_df.toDF(*[c.lower() for c in silver_df.columns])

silver_df = silver_df.cache()


final_df = silver_df.withColumn(
    "time_of_day",
    when(hour(to_timestamp(col("time"), "HH:mm:ss")) < 12, "Morning")
    .when(hour(to_timestamp(col("time"), "HH:mm:ss")) < 16, "Afternoon")
    .when(hour(to_timestamp(col("time"), "HH:mm:ss")) < 20, "Evening")
    .otherwise("Night")
)


final_df = final_df.withColumn(
    "fare_per_distance",
    when(
        (col("booking_status") == "Completed") & (col("ride_distance") > 0),
        (col("booking_value") / col("ride_distance")).cast("double")
    ).otherwise(None)
)

names_array = array(
    lit("Aarav"),
    lit("Riya"),
    lit("Kabir"),
    lit("Ananya")
)

final_df = final_df.withColumn(
    "customer_name",
    element_at(
        names_array,
        (floor(rand()*4) + 1).cast("int")
    )
)

dim_customer = final_df.select(
    col("customer_id"),
    col("customer_name")
).dropDuplicates(["customer_id"])

dim_driver = final_df.select(
    col("driver_id"),
    col("vehicle_type")
).dropDuplicates(["driver_id"])

dim_payment = final_df.select("payment_method").distinct()
dim_payment = dim_payment.withColumn(
    "payment_key",
    abs(hash(col("payment_method")))
)

dim_payment = dim_payment.select("payment_key", "payment_method")

final_df = final_df.join(
    dim_payment,
    on="payment_method",
    how="left"
)


pickup_df = final_df.select(col("pickup_location").alias("location"))
drop_df   = final_df.select(col("drop_location").alias("location"))

location_union = pickup_df.union(drop_df).distinct()

dim_location = location_union.withColumn("location_key", abs(hash(col("location"))))

dim_location = dim_location.select(
    "location_key",
    "location"
)

final_df = final_df \
    .join(dim_location.withColumnRenamed("location","pickup_location"),
          on="pickup_location", how="left") \
    .withColumnRenamed("location_key","pickup_location_key") \
    .join(dim_location.withColumnRenamed("location","drop_location"),
          on="drop_location", how="left") \
    .withColumnRenamed("location_key","drop_location_key")


dim_date = spark.sql("""
SELECT explode(
    sequence(
        to_date('2024-01-01'),
        to_date('2024-12-31'),
        interval 1 day
    )
) as date
""")

dim_date = dim_date \
    .withColumn("year", year(col("date"))) \
    .withColumn("quarter", quarter(col("date"))) \
    .withColumn("month", month(col("date"))) \
    .withColumn("month_name", date_format(col("date"), "MMMM")) \
    .withColumn("day_of_month", dayofmonth(col("date"))) \
    .withColumn("day_of_week", dayofweek(col("date"))) \
    .withColumn("day_name", date_format(col("date"), "EEEE")) \
    .withColumn("week_number", weekofyear(col("date"))) \
    .withColumn("date_key", date_format(col("date"), "yyyyMMdd").cast("int"))

dim_date = dim_date.select(
    "date_key",
    "date",
    "year",
    "quarter",
    "month",
    "month_name",
    "day_of_month",
    "day_of_week",
    "day_name",
    "week_number"
)

final_df = final_df.withColumn(
    "date",
    to_date(col("date"), "dd-MM-yyyy")
)


final_df = final_df.join(
    dim_date.select("date", "date_key"),
    on="date",
    how="left"
)


fact_rides = final_df.select(
    "booking_id",
    "customer_id",
    "driver_id",
    "date_key",
    "time",
    "booking_status",
    "vehicle_type",
    "pickup_location_key",
    "drop_location_key",
    "avg_vtat",
    "avg_ctat",
    "cancelled_rides_by_customer",
    "reason_for_cancelling_by_customer",
    "cancelled_rides_by_driver",
    "driver_cancellation_reason",
    "incomplete_rides",
    "incomplete_rides_reason",
    "booking_value",
    "ride_distance",
    "driver_ratings",
    "customer_rating",
    "payment_key",
    "time_of_day",
    "fare_per_distance"
)

RIDES_PATH = GOLD_PATH + "fact_rides/"
CUSTOMER_PATH = GOLD_PATH + "dim_customer/"
DRIVER_PATH = GOLD_PATH + "dim_driver/"
LOCATION_PATH = GOLD_PATH + "dim_location/"
DATE_PATH = GOLD_PATH + "dim_date/"
PAYMENT_PATH = GOLD_PATH + "dim_payment/"

def idempotent_append(df, path, key_cols):
    try:
        existing_df = spark.read.parquet(path)

        df_new = df.join(
            existing_df.select(*key_cols),
            on=key_cols,
            how="left_anti"
        )

    except:
        # first run
        df_new = df

    df_new.write.mode("append").parquet(path)


idempotent_append(fact_rides, RIDES_PATH, ["booking_id"])
idempotent_append(dim_customer, CUSTOMER_PATH, ["customer_id"])
idempotent_append(dim_driver, DRIVER_PATH, ["driver_id"])
idempotent_append(dim_location, LOCATION_PATH, ["location"])
dim_date.write.mode("overwrite").parquet(DATE_PATH)
dim_payment.write.mode("overwrite").parquet(PAYMENT_PATH)
