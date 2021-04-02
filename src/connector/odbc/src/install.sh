#!/bin/bash

set -u

EXT="so"
[[ `uname` == 'Darwin' ]] && EXT="dylib"

SUDO="sudo"
[[ `uname` == 'Darwin' ]] && SUDO=""


BLD_DIR="$1"

rm -f "${BLD_DIR}/template.ini"
rm -f "${BLD_DIR}/template.dsn"

cat > "${BLD_DIR}/template.ini" <<EOF
[TAOS]
Description=taos odbc driver
Driver=${BLD_DIR}/build/lib/libtodbc.${EXT}
EOF

cat > "${BLD_DIR}/template.dsn" <<EOF
[TAOS_DSN]
Description=Connection to TAOS
Driver=TAOS
// UID=
// PWD=
// Server=localhost:6030

// https://www.npmjs.com/package/odbc
// SQL_C_FLOAT not support yet for node odbc, thus could promote to SQL_DOUBLE
// workaround:
// map.float=SQL_DOUBLE

// pyodbc: https://github.com/mkleehammer/pyodbc
// bigint seems not working properly
// workaround:
// map.bigint=SQL_CHAR
EOF

# better remove first ?
${SUDO} odbcinst -i -d -f "${BLD_DIR}/template.ini" &&
odbcinst -i -s -f "${BLD_DIR}/template.dsn" &&
echo "odbc install done"

