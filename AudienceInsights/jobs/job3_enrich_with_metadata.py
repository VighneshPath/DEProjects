from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("EnrichWithMetadata").getOrCreate()

DATA_PATH = "../data"
OUTPUT_PATH = "../output"

clicks = spark.read.parquet(f"{OUTPUT_PATH}/stage2_cleaned")
apps = spark.read.option("header", True).csv(f"{DATA_PATH}/app_metadata.csv")
devices = spark.read.option("header", True).csv(f"{DATA_PATH}/device_info.csv")
campaigns = spark.read.option("header", True).csv(f"{DATA_PATH}/campaign_metadata.csv")

enriched = clicks \
    .join(apps, on="app", how="left") \
    .join(devices, on="device", how="left") \
    .join(campaigns, on="channel", how="left")

enriched.write.mode("overwrite").parquet(f"{OUTPUT_PATH}/stage3_enriched")