import datetime
import random

start_time = datetime.datetime(2025, 10, 10, 12, 0, 0)
num_rows = 10000
true_row_indexes = (2, 1000)
false_index = 8500

for i in range(num_rows):
    current_time = start_time + datetime.timedelta(seconds=i)
    bool_val = 'true' if i in true_row_indexes else 'false' if i == false_index else 'null'
    double_val = 1 if i in true_row_indexes else 0 if i == false_index else -1
    with open('data3-3.csv', 'a') as f:
        f.write(f"'{current_time.strftime('%Y-%m-%d %H:%M:%S')}',{bool_val},{double_val}\n")
