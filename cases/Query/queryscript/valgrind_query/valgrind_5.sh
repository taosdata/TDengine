#!/bin/bash

valgrind --log-file=/root/valgrind/5/t1.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_query/split/stable_query_2.py --keep  --disable_collection
valgrind --log-file=/root/valgrind/5/t2.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_query/stable_query_null.py --keep  --disable_collection
valgrind --log-file=/root/valgrind/5/t3.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/time/split/stable_time_to_iso8601_2.py --keep --disable_collection
valgrind --log-file=/root/valgrind/5/t4.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/time/split/stable_time_to_unixtimestamp_1.py --keep --disable_collection
valgrind --log-file=/root/valgrind/5/t5.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/str/stable_str_lower.py --keep --disable_collection
valgrind --log-file=/root/valgrind/5/t6.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/str/stable_str_upper.py --keep --disable_collection
valgrind --log-file=/root/valgrind/5/t7.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/str/stable_str_ltrim.py --keep --disable_collection
valgrind --log-file=/root/valgrind/5/t8.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/numeric/table_numeric_table_only_stable_groupby.py --keep --disable_collection
valgrind --log-file=/root/valgrind/5/t9.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/str/split/table_str_rtrim_2.py --keep --disable_collection
valgrind --log-file=/root/valgrind/5/t10.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/str/table_str_substr.py --keep --disable_collection
valgrind --log-file=/root/valgrind/5/t11.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/str/table_taos_f.py --keep --disable_collection
valgrind --log-file=/root/valgrind/5/t12.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/str/table_str_length.py --keep --disable_collection
valgrind --log-file=/root/valgrind/5/t13.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/numeric/table_numeric_interval.py --keep --disable_collection


wait