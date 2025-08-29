python3 test.py -f tools/benchmark/basic/insertMix.py
python3 ./test.py -f tools/benchmark/basic/insertBindVGroup.py
python3 ./test.py -f tools/benchmark/basic/insert-json-csv.py
python3 ./test.py -f tools/benchmark/basic/stmt2_insert.py
python3 ./test.py -f tools/benchmark/basic/query_json-with-sqlfile.py

python3 test.py -f tools\taosdump\native\taosdumpSchemaChange.py
python3 ./test.py -f tools/taosdump/native/taosdumpCompa.py
python3 ./test.py -f tools/taosdump/native/taosdumpTestBasic.py
python3 ./test.py -f tools/taosdump/native/taosdumpManyCols.py
python3 ./test.py -f tools/taosdump/native/taosdumpTestTypeJson.py