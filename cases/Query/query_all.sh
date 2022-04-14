taostest --setup=test_217env.yaml

#table_query
taostest --use=test_217env.yaml --case=Query/queryscript/table_query/table_query.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/table_query/table_query_null.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/table_query/table_query_union.py --keep

#stable_query
taostest --use=test_217env.yaml --case=Query/queryscript/stable_query/stable_query.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_query/stable_query_null.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_query/stable_query_union.py --keep

#stable_fun_time_query
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/time/stable_time_now.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/time/stable_time_today.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/time/stable_time_zone.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/time/stable_time_to_iso8601.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/time/stable_time_to_unixtimestamp.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/time/stable_time_truncate.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/time/stable_time_timediff.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/time/stable_time_elapsed.py --keep

#stable_fun_str_query
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/str/stable_str_base.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/str/stable_str_upper.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/str/stable_str_lower.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/str/stable_str_ltrim.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/str/stable_str_rtrim.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/str/stable_str_length.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/str/stable_str_substr.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/str/stable_str_concat.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/str/stable_str_concat_ws.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/str/stable_str_cast.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/str/stable_str_interval.py --keep