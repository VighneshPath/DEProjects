from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ExportKPIs").getOrCreate()

OUTPUT_PATH = "../output"
EXPORT_PATH = "../export"

kpis = spark.read.option("header", True).csv(f"{OUTPUT_PATH}/kpi_clicks_by_country_app")
kpis.write.mode("overwrite").json(f"{EXPORT_PATH}/final_kpis.json")