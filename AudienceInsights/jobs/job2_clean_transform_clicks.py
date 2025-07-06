from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp

spark = SparkSession.builder.appName("CleanTransformClicks").getOrCreate()

OUTPUT_PATH = "../output"

clickstream = spark.read.parquet(f"{OUTPUT_PATH}/stage1_clickstream")
cleaned = clickstream.withColumn("click_time", to_timestamp("click_time", "MM/dd/yyyy HH:mm"))
cleaned.write.mode("overwrite").parquet(f"{OUTPUT_PATH}/stage2_cleaned")
