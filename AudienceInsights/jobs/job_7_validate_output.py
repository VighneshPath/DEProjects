from pyspark.sql import SparkSession
import sys
import os

spark = SparkSession.builder.appName("ValidatePipelineOutput").getOrCreate()

OUTPUT_PATH = "../output"
STAGE = "stage5_kpis"
output_path = os.path.join(OUTPUT_PATH, STAGE)

try:
    df = spark.read.json(output_path)
    record_count = df.count()

    if record_count == 0:
        raise ValueError(f"[VALIDATION FAILED] No records found in final output at {output_path}.")
    else:
        print(f"[VALIDATION PASSED] Output contains {record_count} records.")

except Exception as e:
    print(f"[ERROR] Failed to read/validate output from {output_path}: {e}")
    sys.exit(1)
