#!/bin/bash

valgrind --log-file=/root/valgrind/7/t1.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/time/split/stable_time_now_1.py --keep --disable_collection
valgrind --log-file=/root/valgrind/7/t2.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/time/split/stable_time_today_2.py --keep --disable_collection
valgrind --log-file=/root/valgrind/7/t3.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/time/split/stable_time_zone_1.py --keep --disable_collection
valgrind --log-file=/root/valgrind/7/t4.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/time/split/stable_time_timediff_2.py --keep --disable_collection
valgrind --log-file=/root/valgrind/7/t5.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/time/split/stable_time_elapsed_1.py --keep --disable_collection
valgrind --log-file=/root/valgrind/7/t6.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/numeric/split/stable_numeric_stddev_2.py --keep --disable_collection
valgrind --log-file=/root/valgrind/7/t7.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/str/stable_str_interval.py --keep --disable_collection
valgrind --log-file=/root/valgrind/7/t8.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/str/table_str_concat_ws.py --keep --disable_collection
valgrind --log-file=/root/valgrind/7/t9.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/str/table_str_lower.py --keep --disable_collection
valgrind --log-file=/root/valgrind/7/t10.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/time/table_time_to_unixtimestamp.py --keep --disable_collection
valgrind --log-file=/root/valgrind/7/t11.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/numeric/split/stable_numeric_csum_2.py --keep --disable_collection
valgrind --log-file=/root/valgrind/7/t12.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/numeric/split/stable_numeric_max_min_1.py --keep --disable_collection
valgrind --log-file=/root/valgrind/7/t13.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/time/stable_time_interval.py --keep --disable_collection
valgrind --log-file=/root/valgrind/7/t14.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/str/table_str_interval.py --keep --disable_collection

wait