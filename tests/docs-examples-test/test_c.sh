#!/bin/bash

set -e

taosd >> /dev/null 2>&1 &
taosadapter >> /dev/null 2>&1 &

cd ../../docs/examples/c

# 1
gcc connect_example.c -o connect_example -ltaos
./connect_example

# 2
taos -s "drop database if exists power"
gcc -o insert_example insert_example.c -ltaos
./insert_example

gcc -o async_query_example async_query_example.c -ltaos
./async_query_example

# 3
taos -s "drop database if exists power"
gcc -o stmt_example stmt_example.c -ltaos
./stmt_example

# 4
taos -s "drop database if exists power"
gcc -o multi_bind_example multi_bind_example.c -ltaos
./multi_bind_example

# 5
gcc -o query_example query_example.c -ltaos
./query_example

# 6
taos -s "drop database if exists test"
gcc -o line_example line_example.c -ltaos
./line_example

# 7
taos -s "drop database if exists test"
gcc -o telnet_line_example telnet_line_example.c -ltaos
./telnet_line_example 

# 8
taos -s "drop database if exists test"
gcc -o json_protocol_example json_protocol_example.c -ltaos
./json_protocol_example 

