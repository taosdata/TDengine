#!/bin/bash

valgrind --log-file=/root/valgrind/9/t1.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/math/table_math_histogram.py --keep --disable_collection
valgrind --log-file=/root/valgrind/9/t2.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/math/table_math_leastsquares.py --keep --disable_collection
valgrind --log-file=/root/valgrind/9/t3.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/numeric/stable_numeric_state.py --keep --disable_collection
valgrind --log-file=/root/valgrind/9/t4.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/numeric/stable_numeric_mavg.py --keep --disable_collection
valgrind --log-file=/root/valgrind/9/t5.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/alltype/table_alltype_last.py --keep --disable_collection
valgrind --log-file=/root/valgrind/9/t6.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/alltype/table_alltype_last_row.py --keep --disable_collection
valgrind --log-file=/root/valgrind/9/t7.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/numeric/table_numeric_csum.py --keep --disable_collection
valgrind --log-file=/root/valgrind/9/t8.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/numeric/table_numeric_floor_ceil_round.py --keep --disable_collection
valgrind --log-file=/root/valgrind/9/t9.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/numeric/table_numeric_stddev.py --keep --disable_collection
valgrind --log-file=/root/valgrind/9/t10.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/numeric/table_numeric_diff.py --keep --disable_collection
valgrind --log-file=/root/valgrind/9/t11.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/numeric/table_numeric_avg.py --keep --disable_collection
valgrind --log-file=/root/valgrind/9/t12.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/alltype/table_func_error.py --keep --disable_collection
valgrind --log-file=/root/valgrind/9/t13.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/str/split/stable_str_cast_1.py --keep --disable_collection

wait