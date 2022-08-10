#!/bin/bash

valgrind --log-file=/root/valgrind/0/t1.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/time/split/stable_time_to_unixtimestamp_2.py --keep --disable_collection
valgrind --log-file=/root/valgrind/0/t2.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/time/split/stable_time_truncate_1.py --keep --disable_collection
valgrind --log-file=/root/valgrind/0/t3.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/str/stable_str_concat_ws.py --keep --disable_collection
valgrind --log-file=/root/valgrind/0/t4.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/str/stable_str_rtrim.py --keep --disable_collection
valgrind --log-file=/root/valgrind/0/t5.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/str/stable_str_substr.py --keep --disable_collection
valgrind --log-file=/root/valgrind/0/t6.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/numeric/split/stable_numeric_stddev_1.py --keep --disable_collection
valgrind --log-file=/root/valgrind/0/t7.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/str/split/table_str_rtrim_1.py --keep --disable_collection
valgrind --log-file=/root/valgrind/0/t8.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/time/table_time_today.py --keep --disable_collection
valgrind --log-file=/root/valgrind/0/t9.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/time/table_time_now.py --keep --disable_collection
valgrind --log-file=/root/valgrind/0/t10.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/numeric/table_numeric_mavg.py --keep --disable_collection
valgrind --log-file=/root/valgrind/0/t11.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/alltype/stable_alltype_tail.py --keep --disable_collection
valgrind --log-file=/root/valgrind/0/t12.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/alltype/stable_alltype_unique.py --keep --disable_collection
valgrind --log-file=/root/valgrind/0/t13.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/alltype/stable_alltype_mode.py --keep --disable_collection
valgrind --log-file=/root/valgrind/0/t14.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/math/stable_math_sin_cos_tan.py --keep --disable_collection

wait