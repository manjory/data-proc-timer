import csv
import os
import time
from datetime import datetime
from io import StringIO

start = time.perf_counter()

file_path='/Users/macbookpro_2015/dev/python/data-proc-timer/test_data_1000.csv'
output_script_path = '/Users/macbookpro_2015/dev/python/data-proc-timer/filter_response.csv'

file_size = os.path.getsize(file_path)
print(f"File size: {file_size / 1024:.2f} KB")


threshold_date=datetime(2022,1,1)

with open(file_path, "rb") as file:
    raw=file.read()
text = raw.decode("utf-8",errors="replace")
file= StringIO(text)


reader=csv.DictReader(file)
header=reader.fieldnames

filtered_data=[]

for row in reader:
    subs_date=row.get("Subscription Date","")

    try:
        sub_date_obj = datetime.strptime(subs_date, "%Y-%m-%d")
        # print(sub_date_obj)

        if sub_date_obj>=threshold_date:
            filtered_data.append(row)
    except ValueError:
        continue

with open(output_script_path,mode='w',newline='',encoding='utf-8') as output_file:
    writer=csv.DictWriter(output_file,fieldnames=header)
    writer.writeheader()
    writer.writerows(filtered_data)


end=time.perf_counter()

elapsed=end-start

print(f"Processed 1000 rows in {elapsed:.6f} seconds")

rows=1000

rows_per_sec=rows/elapsed

print(f"Throughput:{rows_per_sec: .2f} rows/sec" )


"""

result from this processing:


File size: 167.21 KB
Processed 1000 rows in 0.016084 seconds
Throughput: 62172.94 rows/sec

"""
