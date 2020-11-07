#!/bin/bash

set -u

BLD_DIR="$1"

rm -f "${BLD_DIR}/template.ini"
rm -f "${BLD_DIR}/template.dsn"

cat > "${BLD_DIR}/template.ini" <<EOF
[TAOS]
Description=taos odbc driver
Driver=${BLD_DIR}/build/lib/libtodbc.so
EOF

cat > "${BLD_DIR}/template.dsn" <<EOF
[TAOS_DSN]
Description=Connection to TAOS
Driver=TAOS
Server=localhost:6030
EOF

# better remove first ?
sudo odbcinst -i -d -f "${BLD_DIR}/template.ini" &&
odbcinst -i -s -f "${BLD_DIR}/template.dsn" &&
echo "odbc install done"

