from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col

spark = SparkSession.builder.appName("AggregateInsights").getOrCreate()

OUTPUT_PATH = "../output"

geo_clicks = spark.read.parquet(f"{OUTPUT_PATH}/stage4_geo")

kpis = geo_clicks.groupBy("country", "app").agg(count("ip").alias("clicks"))
kpis.write.mode("overwrite").csv(f"{OUTPUT_PATH}/kpi_clicks_by_country_app", header=True)