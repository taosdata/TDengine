#!/bin/bash
set -e
set -x

python3 ./test.py -f 5-taos-tools/taosbenchmark/auto_create_table_json.py
python3 ./test.py -f 5-taos-tools/taosbenchmark/commandline.py
python3 ./test.py -f 5-taos-tools/taosbenchmark/custom_col_tag.py
python3 ./test.py -f 5-taos-tools/taosbenchmark/default_json.py
python3 ./test.py -f 5-taos-tools/taosbenchmark/demo.py
python3 ./test.py -f 5-taos-tools/taosbenchmark/insert_alltypes_json.py
python3 ./test.py -f 5-taos-tools/taosbenchmark/invalid_commandline.py
python3 ./test.py -f 5-taos-tools/taosbenchmark/json_tag.py
python3 ./test.py -f 5-taos-tools/taosbenchmark/limit_offset_json.py
python3 ./test.py -f 5-taos-tools/taosbenchmark/query_json.py
python3 ./test.py -f 5-taos-tools/taosbenchmark/sample_csv_json.py
python3 ./test.py -f 5-taos-tools/taosbenchmark/sml_interlace.py
python3 ./test.py -f 5-taos-tools/taosbenchmark/sml_json_alltypes.py
python3 ./test.py -f 5-taos-tools/taosbenchmark/sml_telnet_alltypes.py
python3 ./test.py -f 5-taos-tools/taosbenchmark/taosadapter_json.py
python3 ./test.py -f 5-taos-tools/taosbenchmark/telnet_tcp.py
