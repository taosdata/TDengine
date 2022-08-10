#!/bin/bash

valgrind --log-file=/root/valgrind/3/t1.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_query/split/stable_query_union_2_2.py --keep  --disable_collection
valgrind --log-file=/root/valgrind/3/t2.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_query/split/stable_query_union_3.py --keep  --disable_collection
valgrind --log-file=/root/valgrind/3/t3.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/str/stable_str_interval.py --keep --disable_collection
valgrind --log-file=/root/valgrind/3/t4.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/str/table_str_concat_ws.py --keep --disable_collection
valgrind --log-file=/root/valgrind/3/t5.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/str/table_str_lower.py --keep --disable_collection
valgrind --log-file=/root/valgrind/3/t6.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/time/table_time_truncate.py --keep --disable_collection
valgrind --log-file=/root/valgrind/3/t7.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/time/table_time_timediff.py --keep --disable_collection
valgrind --log-file=/root/valgrind/3/t8.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/math/stable_math_abs.py --keep --disable_collection
valgrind --log-file=/root/valgrind/3/t9.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/math/stable_math_interval.py --keep --disable_collection
valgrind --log-file=/root/valgrind/3/t10.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/math/split/stable_math_histogram_1.py --keep --disable_collection
valgrind --log-file=/root/valgrind/3/t11.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/math/table_math_derivative.py --keep --disable_collection
valgrind --log-file=/root/valgrind/3/t12.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/math/table_math_apercentile.py --keep --disable_collection
valgrind --log-file=/root/valgrind/3/t13.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/math/table_math_percentile.py --keep --disable_collection
valgrind --log-file=/root/valgrind/3/t14.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/math/table_math_sqrt.py --keep --disable_collection

wait