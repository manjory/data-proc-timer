from io import StringIO

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

csv_path="/Users/macbookpro_2015/dev/python/data-proc-timer/test_data_1000.csv"
parquet_path="/Users/macbookpro_2015/dev/python/data-proc-timer/test-data_1000.parquet"

with open(csv_path,"rb") as file:
    raw=file.read()

text = raw.decode("utf-8", errors="replace")

df=pd.read_csv(StringIO(text))

table = pa.Table.from_pandas(df)

pq.write_table(table,parquet_path,compression="snappy")

print("done")

