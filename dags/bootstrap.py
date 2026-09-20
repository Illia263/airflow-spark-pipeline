from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
if __name__ == "__main__":
    spark = SparkSession.builder \
    .appName("Converter") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")\
    .config("spark.driver.memory", "8g")\
    .getOrCreate()
    try:
        choosing = input("Enter name of the file you want to convert(e.g 2019-Nov): ")
        file_path = f"data/source/{choosing}.csv"
    except Exception as e:
        print(f"Here is an error {str(e)}!")
    df = spark.read.csv(file_path, header=True)
    unique_users = df.select('user_id').distinct().dropna()
    print("Generating random countries for users...")
    users_with_country = unique_users.withColumn(
        "country",
        F.when(F.rand() < 0.25, F.lit("Portugal"))
         .when(F.rand() < 0.50, F.lit("Brazil"))
         .when(F.rand() < 0.75, F.lit("USA"))
         .otherwise(F.lit("France"))
    )
    print("Writing dim_users to PostgreSQL...")
    users_with_country.write\
        .format("jdbc")\
        .option("url", "jdbc:postgresql://postgres_db:5432/ecommerce_db") \
        .option("dbtable", "dim_users")\
        .option("user", "ecommerce_user")\
        .option("password", "ecommerce_password")\
        .option("driver", "org.postgresql.Driver")\
        .mode("overwrite")\
        .save()
    print("Processing raw events for Data Lake...")
    with_date = df.withColumn(
        "event_date",
        F.to_date("event_time")

    )
    print("Writing partitioned parquet files to Data Lake...")
    with_date.write\
        .partitionBy("event_date")\
        .mode("overwrite")\
        .parquet(f"data/lake/raw/events/{choosing}")
    print("Bootstrapping finished successfully!")