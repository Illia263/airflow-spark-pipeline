from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from datetime import datetime, date
import sys
import os
import psycopg2
if __name__ == "__main__":
    spark = SparkSession.builder\
    .appName("Daily_ecommerce")\
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")\
    .config("spark.driver.memory", "8g")\
    .getOrCreate()
    try:
        date_obj = datetime.strptime(sys.argv[1], "%Y-%m-%d")
        month_folder = date_obj.strftime("%Y-%b")
        file_path = f"/opt/airflow/data/lake/raw/events/{month_folder}/event_date={sys.argv[1]}/"
        if not os.path.exists(file_path):
            print(f"No data found for {sys.argv[1]}. Skipping gracefully.")
            sys.exit(0)
    except Exception as e:
        print(f"Here is an error {str(e)}!")
        sys.exit(1)
    df = spark.read.parquet(file_path)
    grouped_users = df.groupBy('user_id').agg(
        F.count("*").alias("total_events"),
        F.sum("price").alias("total_spent")
    )
    dim_users_df = spark.read\
        .format("jdbc")\
        .option("url", "jdbc:postgresql://ecommerce_dwh:5432/ecommerce_db")\
        .option("dbtable", "dim_users")\
        .option("user", "ecommerce_user")\
        .option("password", "ecommerce_password")\
        .option("driver", "org.postgresql.Driver")\
        .load()
    final_df = grouped_users.join(dim_users_df, on="user_id", how="left")
    final_df.write \
    .format("jdbc")\
    .option("url", "jdbc:postgresql://ecommerce_dwh:5432/ecommerce_db")\
    .option("dbtable", "daily_sales")\
    .option("user", "ecommerce_user")\
    .option("password", "ecommerce_password")\
    .option("driver", "org.postgresql.Driver")\
    .mode("append")\
    .save()