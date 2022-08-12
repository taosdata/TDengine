#!/bin/bash

valgrind --log-file=/root/valgrind/6/t1.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/time/split/stable_time_today_1.py --keep --disable_collection
valgrind --log-file=/root/valgrind/6/t2.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/time/split/stable_time_truncate_2.py --keep --disable_collection
valgrind --log-file=/root/valgrind/6/t3.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/time/split/stable_time_timediff_1.py --keep --disable_collection
valgrind --log-file=/root/valgrind/6/t4.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/numeric/split/stable_numeric_stddev_1_1.py --keep --disable_collection
valgrind --log-file=/root/valgrind/6/t5.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/time/table_time_elapsed.py --keep --disable_collection
valgrind --log-file=/root/valgrind/6/t6.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/time/table_time_to_iso8601.py --keep --disable_collection
valgrind --log-file=/root/valgrind/6/t7.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/alltype/stable_alltype_sample.py --keep --disable_collection
valgrind --log-file=/root/valgrind/6/t8.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/alltype/stable_alltype_count.py --keep --disable_collection
valgrind --log-file=/root/valgrind/6/t9.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/math/stable_math_pow_log.py --keep --disable_collection
valgrind --log-file=/root/valgrind/6/t10.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/math/stable_math_percentile.py --keep --disable_collection
valgrind --log-file=/root/valgrind/6/t11.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/math/stable_math_derivative.py --keep --disable_collection
valgrind --log-file=/root/valgrind/6/t12.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/math/table_math_sin_cos_tan.py --keep --disable_collection
valgrind --log-file=/root/valgrind/6/t13.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/time/table_time_interval.py --keep --disable_collection
valgrind --log-file=/root/valgrind/6/t14.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/math/stable_math_sqrt.py --keep --disable_collection

wait