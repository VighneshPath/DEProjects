from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("GeoEnrichIP").getOrCreate()

OUTPUT_PATH = "../output"

def fake_country(ip):
    # Dummy geo enrichment logic
    return ["US", "IN", "BR", "DE", "FR"][int(ip) % 5] if ip.isdigit() else "UNKNOWN"

country_udf = udf(fake_country, StringType())

clicks = spark.read.parquet(f"{OUTPUT_PATH}/stage3_enriched")
geo_enriched = clicks.withColumn("country", country_udf(col("ip")))
geo_enriched.write.mode("overwrite").parquet(f"{OUTPUT_PATH}/stage4_geo")