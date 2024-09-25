# Copyright © 2020 BangxinIot Technology Co.,Ltd. All rights reserved.
# None of the materials provided in this project (lib) may be reproduced or transmitted in whole or in part,
# in any form or by any means, electronic or mechanical, including photocopying, recording, or the use of
# any information storage and retrieval system, except as provided in the Terms and Conditions of Contract
# or agreement from BangxinIot. It is forbidden to use our technology for patent or software copyright 
# applications, except as permitted by the agreement. For permissions or further enquiries, visit:
# https://mathearth.com


import taos
import random
import time

# Target database settings for TDengine
target_ip = "127.0.0.1"
target_port = 6030
target_user = "root"
target_password = "taosdata"

def generate_data_batch(start_index, batch_size, start_ts, freq_hz, mv_range):
    interval_ms = 1000 // freq_hz
    data = []
    for i in range(batch_size):
        ts = start_ts + (start_index + i) * interval_ms
        mv = random.randint(*mv_range)
        data.append((ts, mv))
    return data

def insert_data_in_batches(table_name, total_records, batch_size, start_ts, freq_hz, mv_range):
    # Database connection
    conn = taos.connect(host=target_ip, port=target_port, user=target_user, password=target_password)
    cursor = conn.cursor()
    print(cursor)
    try:
        for start_index in range(0, total_records, batch_size):
            data_batch = generate_data_batch(start_index, min(batch_size, total_records - start_index),
                                             start_ts, freq_hz, mv_range)
            values = ', '.join([f"('{time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(ts/1000))}.{int(ts%1000):03}', {mv})"
                                for ts, mv in data_batch])
            sql = f"INSERT INTO {table_name} (ts, mv) VALUES {values};"
            print(sql)
            cursor.execute(sql)
            print(f"Inserted batch starting at index {start_index} into {table_name}")
    except Exception as e:
        print(f"Failed to insert data into {table_name}: {e}")
    finally:
        cursor.close()
        # Close connection
        conn.close()


def get_subtables(conn, super_table):
    cursor = conn.cursor()
    try:
        # Show table tags for super table
        cursor.execute(f"SHOW TABLE TAGS FROM {super_table};")
        # Assuming the first column is the subtable name
        subtables = [row[0] for row in cursor.fetchall()]
        return subtables
    except Exception as e:
        print(f"Failed to retrieve subtables for {super_table}: {e}")
    finally:
        cursor.close()

def main():

    # Parameters
    total_records = 6000
    batch_size = 10000  # Limit per SQL statement
    freq_hz = 100
    mv_range = (2000, 3000)

    # Get all subtables from a super table
    dbname = "DEFAULT_MEIOT_1"
    super_table_name = "t_lingang_stable_sensor_shadow_pressure_1"
    super_table = f"{dbname}.{super_table_name}"

    # Database connection
    conn = taos.connect(host=target_ip, port=target_port, user=target_user, password=target_password)
    cursor = conn.cursor()
    subtables = get_subtables(conn, super_table)
    # Close connection
    conn.close()

    while 1:
        # Insert data into all subtables in batches
        for table_name in subtables:
            print(table_name)
            table = f"{dbname}.{table_name}"
            now_ts = int(time.time() * 1000)  # Current timestamp in milliseconds
            # 选择一分钟前的时间戳
            start_ts = now_ts - 60000
            insert_data_in_batches(table, total_records, batch_size, start_ts, freq_hz, mv_range)
        time.sleep(1)


if __name__ == "__main__":
    main()