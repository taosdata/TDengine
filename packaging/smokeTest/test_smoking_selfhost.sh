#!/bin/bash

# Define log file and result files
LOG_FILE="test_server.log"
SUCCESS_FILE="success.txt"
FAILED_FILE="failed.txt"

# Initialize/clear result files
> "$SUCCESS_FILE"
> "$FAILED_FILE"
> "$LOG_FILE"

# Switch to the target directory
TARGET_DIR="../../tests/system-test/"

echo "===== Changing Directory to $TARGET_DIR =====" | tee -a "$LOG_FILE"

if cd "$TARGET_DIR"; then
    echo "Successfully changed directory to $TARGET_DIR" | tee -a "$LOG_FILE"
else
    echo "ERROR: Failed to change directory to $TARGET_DIR" | tee -a "$LOG_FILE"
    exit 1
fi

# Define the Python commands to execute ：case list
commands=(
    "python3 ./test.py -f 2-query/join.py"
    "python3 ./test.py -f 1-insert/insert_column_value.py"
    "python3 ./test.py -f 2-query/primary_ts_base_5.py"
    "python3 ./test.py -f 2-query/case_when.py"
    "python3 ./test.py -f 2-query/partition_limit_interval.py"
    "python3 ./test.py -f 2-query/fill.py"
    "python3 ./test.py -f query/query_basic.py -N 3"
    "python3 ./test.py -f 7-tmq/basic5.py"
    "python3 ./test.py -f 8-stream/stream_basic.py"
    "python3 ./test.py -f 6-cluster/5dnode3mnodeStop.py -N 5 -M 3"
)

# Counters
total=${#commands[@]}
success_count=0
fail_count=0

# Execute each command
for cmd in "${commands[@]}"
do
    echo "===== Executing Command: $cmd =====" | tee -a "$LOG_FILE"
    # Execute the command and append output and errors to the log file
    eval "$cmd" >> "$LOG_FILE" 2>&1
    exit_code=$?
    
    if [ $exit_code -eq 0 ]; then
        echo "SUCCESS: $cmd" | tee -a "$LOG_FILE"
        echo "$cmd" >> "$SUCCESS_FILE"
        ((success_count++))
    else
        echo "FAILED: $cmd" | tee -a "$LOG_FILE"
        echo "$cmd" >> "$FAILED_FILE"
        ((fail_count++))
    fi
    echo "" | tee -a "$LOG_FILE"  # Add an empty line for separation
done

# Generate the final report
echo "===== Test Completed =====" | tee -a "$LOG_FILE"
echo "Total Commands Executed: $total" | tee -a "$LOG_FILE"
echo "Successful: $success_count" | tee -a "$LOG_FILE"
echo "Failed: $fail_count" | tee -a "$LOG_FILE"

if [ $fail_count -ne 0 ]; then
    echo "" | tee -a "$LOG_FILE"
    echo "The following commands failed:" | tee -a "$LOG_FILE"
    cat "$FAILED_FILE" | tee -a "$LOG_FILE"
else
    echo "All commands executed successfully." | tee -a "$LOG_FILE"
fi

# Optional: Generate a separate report file
echo "" > "report.txt"
echo "===== Test Report =====" >> "report.txt"
echo "Total Commands Executed: $total" >> "report.txt"
echo "Successful: $success_count" >> "report.txt"
echo "Failed: $fail_count" >> "report.txt"

if [ $fail_count -ne 0 ]; then
    echo "" >> "report.txt"
    echo "The following commands failed:" >> "report.txt"
    cat "$FAILED_FILE" >> "report.txt"
else
    echo "All commands executed successfully." >> "report.txt"
fi

echo "Detailed logs can be found in $LOG_FILE"
echo "Test report can be found in report.txt"