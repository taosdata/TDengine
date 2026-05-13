#!/bin/bash

valgrind --log-file=/root/valgrind/2/t1.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_query/split/stable_query_union_1.py --keep  --disable_collection
valgrind --log-file=/root/valgrind/2/t2.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_query/split/stable_query_union_2.py --keep  --disable_collection
valgrind --log-file=/root/valgrind/2/t3.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/time/split/stable_time_now_2.py --keep --disable_collection
valgrind --log-file=/root/valgrind/2/t4.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/numeric/split/stable_numeric_stddev_1_1.py --keep --disable_collection
valgrind --log-file=/root/valgrind/2/t5.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/str/table_str_upper.py --keep --disable_collection
valgrind --log-file=/root/valgrind/2/t6.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/str/stable_str_base.py --keep --disable_collection
valgrind --log-file=/root/valgrind/2/t7.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/str/stable_str_length.py --keep --disable_collection
valgrind --log-file=/root/valgrind/2/t8.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/time/table_time_zone.py --keep --disable_collection
valgrind --log-file=/root/valgrind/2/t9.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/alltype/stable_alltype_first.py --keep --disable_collection
valgrind --log-file=/root/valgrind/2/t10.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/alltype/stable_alltype_last.py --keep --disable_collection
valgrind --log-file=/root/valgrind/2/t11.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/numeric/stable_numeric_interval.py --keep --disable_collection
valgrind --log-file=/root/valgrind/2/t12.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/numeric/split/stable_numeric_csum_1.py --keep --disable_collection
valgrind --log-file=/root/valgrind/2/t13.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/math/stable_math_apercentile.py --keep --disable_collection
valgrind --log-file=/root/valgrind/2/t14.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/str/split/stable_str_cast_2.py --keep --disable_collection

wait