#!/bin/bash

valgrind --log-file=/root/valgrind/1/t1.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/str/table_str_concat.py --keep  --disable_collection
valgrind --log-file=/root/valgrind/1/t2.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/str/split/stable_str_concat_2.py --keep  --disable_collection
valgrind --log-file=/root/valgrind/1/t3.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_query/table_query_union.py --keep --disable_collection
valgrind --log-file=/root/valgrind/1/t4.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_query/table_query_null.py --keep --disable_collection
valgrind --log-file=/root/valgrind/1/t5.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_query/table_query.py --keep --disable_collection
valgrind --log-file=/root/valgrind/1/t6.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/numeric/stable_numeric_avg.py --keep --disable_collection
valgrind --log-file=/root/valgrind/1/t7.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/numeric/stable_numeric_sum.py --keep --disable_collection
valgrind --log-file=/root/valgrind/1/t8.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/numeric/stable_numeric_spread.py --keep --disable_collection
valgrind --log-file=/root/valgrind/1/t9.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/numeric/stable_numeric_floor_ceil_round.py --keep --disable_collection
valgrind --log-file=/root/valgrind/1/t10.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/numeric/table_numeric_sum.py --keep --disable_collection
valgrind --log-file=/root/valgrind/1/t11.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/numeric/table_numeric_max_min.py --keep --disable_collection
valgrind --log-file=/root/valgrind/1/t12.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/numeric/table_numeric_spread.py --keep --disable_collection
valgrind --log-file=/root/valgrind/1/t13.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/math/stable_math_sin_cos_tan.py --keep --disable_collection

wait