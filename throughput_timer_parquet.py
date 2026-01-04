import pandas as pd
import time
from datetime import datetime

start = time.perf_counter()

file_path = "/Users/macbookpro_2015/dev/python/data-proc-timer/test-data_1000.parquet"
output_path = "/Users/macbookpro_2015/dev/python/data-proc-timer/filter_response.parquet"

# read parquet
df = pd.read_parquet(file_path)

# filter
threshold_date = datetime(2022, 1, 1)
df["Subscription Date"] = pd.to_datetime(df["Subscription Date"], errors="coerce")
filtered_df = df[df["Subscription Date"] >= threshold_date]

# write parquet
filtered_df.to_parquet(output_path, compression="snappy", index=False)

end = time.perf_counter()
elapsed = end - start

print(f"File size: {len(df)} rows")
print(f"Processed {len(filtered_df)} rows in {elapsed:.6f} seconds")

rows_per_sec = len(df) / elapsed
print(f"Throughput: {rows_per_sec:.2f} rows/sec")


"""
File size: 1000 rows
Processed 170 rows in 0.193072 seconds
Throughput: 5179.41 rows/sec


"""
