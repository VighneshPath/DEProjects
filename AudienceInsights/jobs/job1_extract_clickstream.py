from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ExtractClickstream").getOrCreate()

DATA_PATH = "../data"
OUTPUT_PATH = "../output"

clickstream = spark.read.option("header", True).csv(f"{DATA_PATH}/talking_data_clickstream.csv")
clickstream.write.mode("overwrite").parquet(f"{OUTPUT_PATH}/stage1_clickstream")