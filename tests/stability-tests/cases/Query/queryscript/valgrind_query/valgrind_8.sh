#!/bin/bash

valgrind --log-file=/root/valgrind/8/t1.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/time/split/stable_time_zone_2.py --keep --disable_collection
valgrind --log-file=/root/valgrind/8/t2.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/time/split/stable_time_to_iso8601_1.py --keep --disable_collection
valgrind --log-file=/root/valgrind/8/t3.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/time/split/stable_time_elapsed_2.py --keep --disable_collection
valgrind --log-file=/root/valgrind/8/t4.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/str/table_str_ltrim.py --keep --disable_collection
valgrind --log-file=/root/valgrind/8/t5.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/str/table_str_base.py --keep --disable_collection
valgrind --log-file=/root/valgrind/8/t6.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/str/table_str_cast.py --keep --disable_collection
valgrind --log-file=/root/valgrind/8/t7.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/alltype/table_alltype_sample.py --keep --disable_collection
valgrind --log-file=/root/valgrind/8/t8.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/alltype/table_alltype_hyperloglog.py --keep --disable_collection
valgrind --log-file=/root/valgrind/8/t9.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/alltype/table_alltype_count.py --keep --disable_collection
valgrind --log-file=/root/valgrind/8/t10.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/alltype/table_alltype_first.py --keep --disable_collection
valgrind --log-file=/root/valgrind/8/t11.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/numeric/split/stable_numeric_max_min_2.py --keep --disable_collection
valgrind --log-file=/root/valgrind/8/t12.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/stable_function/numeric/stable_numeric_diff.py --keep --disable_collection
valgrind --log-file=/root/valgrind/8/t13.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/numeric/table_numeric_top_bottom.py --keep --disable_collection
valgrind --log-file=/root/valgrind/8/t14.txt taostest  --use=common_cluster_30.yaml --case=Query/queryscript/table_function/numeric/table_numeric_state.py --keep --disable_collection
wait