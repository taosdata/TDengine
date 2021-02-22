python3 ./test.py -f update/merge_commit_data-0.py
# wal
python3 ./test.py -f wal/addOldWalTest.py

# function
python3 ./test.py -f functions/all_null_value.py
# functions
python3 ./test.py -f functions/function_avg.py -r 1
python3 ./test.py -f functions/function_bottom.py -r 1
python3 ./test.py -f functions/function_count.py -r 1
python3 ./test.py -f functions/function_diff.py -r 1
python3 ./test.py -f functions/function_first.py -r 1
python3 ./test.py -f functions/function_last.py -r 1
python3 ./test.py -f functions/function_last_row.py -r 1
python3 ./test.py -f functions/function_leastsquares.py -r 1
python3 ./test.py -f functions/function_max.py -r 1
python3 ./test.py -f functions/function_min.py -r 1
python3 ./test.py -f functions/function_operations.py -r 1 
python3 ./test.py -f functions/function_percentile.py -r 1
python3 ./test.py -f functions/function_spread.py -r 1
python3 ./test.py -f functions/function_stddev.py -r 1
python3 ./test.py -f functions/function_sum.py -r 1
python3 ./test.py -f functions/function_top.py -r 1
python3 ./test.py -f functions/function_twa.py -r 1
python3 ./test.py -f functions/function_twa_test2.py
python3 ./test.py -f functions/function_stddev_td2555.py
python3 ./test.py -f insert/metadataUpdate.py
python3 ./test.py -f tools/taosdemoTest2.py
python3 ./test.py -f query/last_cache.py
python3 ./test.py -f query/last_row_cache.py
python3 ./test.py -f account/account_create.py
python3 ./test.py -f alter/alter_table.py
python3 ./test.py -f query/queryGroupbySort.py

python3 ./test.py -f insert/unsignedInt.py
python3 ./test.py -f insert/unsignedBigint.py
python3 ./test.py -f insert/unsignedSmallint.py
python3 ./test.py -f insert/unsignedTinyint.py
python3 ./test.py -f query/filterAllUnsignedIntTypes.py