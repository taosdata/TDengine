#!/bin/bash

valgrind --log-file=/root/valgrind/4/t1.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_query/split/stable_query_union_4.py --keep  --disable_collection
valgrind --log-file=/root/valgrind/4/t2.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_query/split/stable_query_1.py --keep  --disable_collection
valgrind --log-file=/root/valgrind/4/t3.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/str/split/stable_str_concat_1.py --keep --disable_collection
valgrind --log-file=/root/valgrind/4/t4.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/numeric/split/stable_numeric_stddev_3.py --keep --disable_collection
valgrind --log-file=/root/valgrind/4/t5.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/numeric/stable_numeric_top_bottom.py --keep --disable_collection
valgrind --log-file=/root/valgrind/4/t6.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/numeric/stable_numeric_table_only_stable_groupby.py --keep --disable_collection
valgrind --log-file=/root/valgrind/4/t7.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/math/split/stable_math_histogram_2.py --keep --disable_collection
valgrind --log-file=/root/valgrind/4/t8.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/math/stable_math_leastsquares.py --keep --disable_collection
valgrind --log-file=/root/valgrind/4/t9.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/math/table_math_pow_log.py --keep --disable_collection
valgrind --log-file=/root/valgrind/4/t10.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/numeric/split/stable_numeric_max_min_2.py --keep --disable_collection
valgrind --log-file=/root/valgrind/4/t11.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/numeric/stable_numeric_diff.py --keep --disable_collection
valgrind --log-file=/root/valgrind/4/t12.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/numeric/table_numeric_top_bottom.py --keep --disable_collection
valgrind --log-file=/root/valgrind/4/t13.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/numeric/table_numeric_state.py --keep --disable_collection
valgrind --log-file=/root/valgrind/4/t14.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/numeric/table_numeric_interval.py --keep --disable_collection
wait