

# update
#python3 ./test.py -f update/allow_update.py
python3 ./test.py -f update/allow_update-0.py
python3 ./test.py -f update/append_commit_data.py
python3 ./test.py -f update/append_commit_last-0.py
python3 ./test.py -f update/append_commit_last.py
#python3 ./test.py -f update/merge_commit_data.py
python3 ./test.py -f update/merge_commit_data-0.py
python3 ./test.py -f update/merge_commit_data2.py
python3 ./test.py -f update/merge_commit_data2_update0.py
python3 ./test.py -f update/merge_commit_last-0.py
python3 ./test.py -f update/merge_commit_last.py
python3 ./test.py -f update/bug_td2279.py

# wal
python3 ./test.py -f wal/addOldWalTest.py

# function
python3 ./test.py -f functions/all_null_value.py