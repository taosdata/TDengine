#!/bin/bash

TAOS_CLI=${TAOS_CLI:-taos}
CFG_DIR="/etc/taos/"
CFG_FILE="${CFG_DIR}/snode_flag"
TEMP_FILE="/tmp/snodes.txt"

# clean up temporary file on script exit
trap "rm -f $TEMP_FILE" EXIT

mkdir -p "$CFG_DIR"

if [ -f "$CFG_FILE" ] && grep -q "^snode 1$" "$CFG_FILE"; then
  # snode already recorded in $CFG_FILE, skip creating.
  exit 0
fi

# check connectivity
if ! $TAOS_CLI -s "select server_status();" >/dev/null 2>&1; then
  exit 1
fi

# check if snode exists
$TAOS_CLI -s "show snodes;" > "$TEMP_FILE" 2>/dev/null
if [ $? -ne 0 ]; then
  exit 1
fi

if grep -q "0 row" "$TEMP_FILE"; then
  # snode does not exist, create it
  $TAOS_CLI -s "create snode on dnode 1;" >/dev/null 2>&1 || \
  { echo "Failed to create snode."; exit 2; }
fi

echo "snode 1" > "$CFG_FILE"