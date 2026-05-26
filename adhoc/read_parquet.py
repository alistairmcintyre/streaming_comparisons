import pandas as pd

# Load the Snappy-compressed Parquet file
df = pd.read_parquet("../../git/data-platform/spark-pipelines/logs/bucket_scores/bucket_status.parquet")

# View the first few rows
print(df.head())