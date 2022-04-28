taostest --setup=test_217env.yaml

#table_query
taostest --use=test_217env.yaml --case=Query/queryscript/table_query/table_query.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/table_query/table_query_null.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/table_query/table_query_union.py --keep

#stable_query
taostest --use=test_217env.yaml --case=Query/queryscript/stable_query/stable_query.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_query/stable_query_null.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_query/stable_query_union.py --keep

#stable_fun_query
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/alltype/stable_func_error.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/alltype/stable_func_right.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/alltype/stable_func_right_tbname.py --keep

#stable_fun_time_query
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/time/stable_time_now.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/time/stable_time_today.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/time/stable_time_zone.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/time/stable_time_to_iso8601.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/time/stable_time_to_unixtimestamp.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/time/stable_time_truncate.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/time/stable_time_timediff.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/time/stable_time_elapsed.py --keep

#table_fun_time_query
taostest --use=test_217env.yaml --case=Query/queryscript/table_function/time/table_time_now.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/table_function/time/table_time_today.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/table_function/time/table_time_zone.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/table_function/time/table_time_to_iso8601.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/table_function/time/table_time_to_unixtimestamp.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/table_function/time/table_time_truncate.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/table_function/time/table_time_timediff.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/table_function/time/table_time_elapsed.py --keep

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

#table_fun_str_query
taostest --use=test_217env.yaml --case=Query/queryscript/table_function/str/table_str_base.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/table_function/str/table_str_upper.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/table_function/str/table_str_lower.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/table_function/str/table_str_ltrim.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/table_function/str/table_str_rtrim.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/table_function/str/table_str_length.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/table_function/str/table_str_substr.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/table_function/str/table_str_concat.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/table_function/str/table_str_concat_ws.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/table_function/str/table_str_cast.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/table_function/str/table_str_interval.py --keep

#stable_fun_math_query
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/math/stable_math_interval.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/math/stable_math_abs.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/math/stable_math_apercentile.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/math/stable_math_derivative.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/math/stable_math_histogram.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/math/stable_math_leastsquares.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/math/stable_math_percentile.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/math/stable_math_pow_log.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/math/stable_math_sin_cos_tan.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/math/stable_math_sqrt.py --keep

#table_fun_math_query
taostest --use=test_217env.yaml --case=Query/queryscript/table_function/math/table_math_interval.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/table_function/math/table_math_abs.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/table_function/math/table_math_apercentile.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/table_function/math/table_math_derivative.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/table_function/math/table_math_histogram.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/table_function/math/table_math_leastsquares.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/table_function/math/table_math_percentile.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/table_function/math/table_math_pow_log.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/table_function/math/table_math_sin_cos_tan.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/table_function/math/table_math_sqrt.py --keep

#stable_fun_numeric_query
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/numeric/stable_numeric_interval.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/numeric/stable_numeric_csum.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/numeric/stable_numeric_diff.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/numeric/stable_numeric_floor_ceil_round.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/numeric/stable_numeric_mavg.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/numeric/stable_numeric_state.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/numeric/stable_numeric_stddev.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/numeric/stable_numeric_table_only_stable_groupby.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/numeric/stable_numeric_top_bottom.py --keep

#table_fun_numeric_query
taostest --use=test_217env.yaml --case=Query/queryscript/table_function/numeric/table_numeric_interval.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/table_function/numeric/table_numeric_csum.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/table_function/numeric/table_numeric_diff.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/table_function/numeric/table_numeric_floor_ceil_round.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/table_function/numeric/table_numeric_mavg.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/table_function/numeric/table_numeric_state.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/table_function/numeric/table_numeric_stddev.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/table_function/numeric/table_numeric_table_only_stable_groupby.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/table_function/numeric/table_numeric_top_bottom.py --keep

#stable_fun_alltype_query
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/alltype/stable_alltype_unique.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/alltype/stable_alltype_tail.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/alltype/stable_alltype_sample.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/alltype/stable_alltype_mode.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_function/alltype/stable_alltype_hyperloglog.py --keep
