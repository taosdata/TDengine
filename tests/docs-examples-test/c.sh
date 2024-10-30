#!/bin/bash

pgrep taosd || taosd >> /dev/null 2>&1 &
pgrep taosadapter || taosadapter >> /dev/null 2>&1 &

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

TEST_PATH="../../docs/examples/c"
echo "setting TEST_PATH: $TEST_PATH" 

cd "${TEST_PATH}" || { echo -e "${RED}Failed to change directory to ${TEST_PATH}${NC}"; exit 1; }

LOG_FILE="docs-c-test-out.log"

> $LOG_FILE

make > "$LOG_FILE" 2>&1

if [ $? -eq 0 ]; then
    echo -e "${GREEN}Make completed successfully.${NC}"
else
    echo -e "${RED}Make failed. Check log file: $LOG_FILE${NC}"
    cat "$LOG_FILE"
    exit 1
fi


declare -a TEST_EXES=(
    "connect_example"
    "create_db_demo"
    "insert_data_demo"
    "query_data_demo"
    "with_reqid_demo"
    "stmt_insert_demo"
    "tmq_demo"
    "sml_insert_demo"
)

declare -a NEED_CLEAN=(
    "true"
    "false"
    "false"
    "false"
    "false"
    "false"
    "false"
    "true"
)

totalCases=0
totalFailed=0
totalSuccess=0

for i in "${!TEST_EXES[@]}"; do
    TEST_EXE="${TEST_EXES[$i]}"
    NEED_CLEAN_FLAG="${NEED_CLEAN[$i]}"

    if [ "$NEED_CLEAN_FLAG" = "true" ]; then
        echo "Cleaning database before executing $TEST_EXE..."
        taos -s "drop database if exists power" >> $LOG_FILE 2>&1
    fi

    echo "Executing $TEST_EXE..."
    ./$TEST_EXE >> $LOG_FILE 2>&1
    RESULT=$?

    if [ "$RESULT" -eq 0 ]; then
        totalSuccess=$((totalSuccess + 1))
        echo "[$GREEN OK $NC] $TEST_EXE executed successfully."
    else
        totalFailed=$((totalFailed + 1))
        echo "[$RED FAILED $NC] $TEST_EXE exited with code $RESULT."
    fi
    
    totalCases=$((totalCases + 1))
done

tail -n 40 $LOG_FILE

echo -e "\nTotal number of cases executed: $totalCases"
if [ "$totalSuccess" -gt "0" ]; then
  echo -e "\n${GREEN} ### Total $totalSuccess C case(s) succeed! ### ${NC}"
fi

if [ "$totalFailed" -ne "0" ]; then
  echo -e "\n${RED} ### Total $totalFailed C case(s) failed! ### ${NC}"
  exit 1
fi

echo "All tests completed."